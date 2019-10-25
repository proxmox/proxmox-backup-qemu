use failure::*;
use std::collections::HashSet;
use std::thread::JoinHandle;
use std::sync::{Mutex, Arc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};

use futures::future::{Future, Either, FutureExt};

use proxmox_backup::tools::BroadcastFuture;
use proxmox_backup::backup::{CryptConfig, load_and_decrtypt_key};

use crate::capi_types::*;
use crate::commands::*;

pub(crate) struct BackupTask {
    pub worker: JoinHandle<Result<BackupTaskStats, Error>>,
    pub command_tx: Sender<BackupMessage>,
    pub aborted: Option<String>,  // set on abort, conatins abort reason
}

#[derive(Debug)]
pub(crate) struct BackupTaskStats {
    written_bytes: u64,
}

impl BackupTask {

    pub fn new(setup: BackupSetup) -> Result<Self, Error> {

        let crypt_config = match setup.keyfile {
            None => None,
            Some(ref path) => {
                let (key, _) = load_and_decrtypt_key(path, & || {
                    match setup.key_password {
                        Some(ref key_password) => Ok(key_password.as_bytes().to_vec()),
                        None => bail!("no key_password specified"),
                    }
                })?;
                Some(Arc::new(CryptConfig::new(key)?))
            }
        };

        let (connect_tx, connect_rx) = channel(); // sync initial server connect

        let (command_tx, command_rx) = channel();

        let worker = std::thread::spawn(move ||  {
            backup_worker_task(setup, crypt_config, connect_tx, command_rx)
        });

        let _worker_start_result = connect_rx.recv()??;

        Ok(BackupTask {
            worker,
            command_tx,
            aborted: None,
        })
    }
}

fn handle_async_command<F: 'static + Send + Future<Output=Result<(), Error>>>(
    command_future: F,
    abort_future: impl 'static + Send + Future<Output=Result<(), Error>>,
    callback_info: CallbackPointers,
) -> impl Future<Output = ()> {

    futures::future::select(command_future.boxed(), abort_future.boxed())
        .map(move |either| {
            match either {
                Either::Left((result, _)) => {
                    callback_info.send_result(result);
                }
                Either::Right(_) => { // aborted
                    callback_info.send_result(Err(format_err!("worker aborted")));
                }
            }
        })
}

fn backup_worker_task(
    setup: BackupSetup,
    crypt_config: Option<Arc<CryptConfig>>,
    connect_tx: Sender<Result<(), Error>>,
    command_rx: Receiver<BackupMessage>,
) -> Result<BackupTaskStats, Error>  {

    let mut builder = tokio::runtime::Builder::new();

    builder.blocking_threads(1);
    builder.core_threads(4);
    builder.name_prefix("proxmox-backup-qemu-");

    let runtime = match builder.build() {
        Ok(runtime) => runtime,
        Err(err) =>  {
            connect_tx.send(Err(format_err!("create runtime failed: {}", err)))?;
            bail!("create runtime failed");
        }
    };

    connect_tx.send(Ok(()))?;
    drop(connect_tx); // no longer needed

    let mut client = None;

    let (mut abort_tx, mut abort_rx) = tokio::sync::mpsc::channel(1);
    let abort_rx = async move {
        match abort_rx.recv().await {
            Some(()) => Ok(()),
            None => bail!("abort future canceled"),
        }
    };

    let abort = BroadcastFuture::new(Box::new(abort_rx));

    let written_bytes = Arc::new(AtomicU64::new(0));
    let written_bytes2 = written_bytes.clone();

    let known_chunks = Arc::new(Mutex::new(HashSet::new()));

    let chunk_size = setup.chunk_size;

    runtime.spawn(async move  {

        let registry = Arc::new(Mutex::new(ImageRegistry::new()));

        loop {
            let msg = command_rx.recv().unwrap(); // todo: should be blocking

            match msg {
                BackupMessage::Connect { callback_info } => {
                    client = match setup.connect().await {
                        Ok(client) => {
                            callback_info.send_result(Ok(()));
                            Some(client)
                        }
                        Err(err) => {
                            callback_info.send_result(Err(err));
                            None
                        }
                    };
                }
                BackupMessage::Abort => {
                    println!("worker got abort mesage");
                    let res = abort_tx.send(()).await;
                    if let Err(_err) = res  {
                        println!("sending abort failed");
                    }
                }
                BackupMessage::End => {
                    println!("worker got end mesage");
                    break;
                }
                BackupMessage::AddConfig { name, data, size, result_channel } => {
                    match client {
                        Some(ref client) => {
                            let res = add_config(
                                client.clone(),
                                crypt_config.clone(),
                                registry.clone(),
                                name,
                                data,
                                size,
                            ).await;

                            let _ = result_channel.lock().unwrap().send(res);
                        }
                        None => {
                           let _ = result_channel.lock().unwrap().send(Err(format_err!("not connected")));
                        }
                    }
                }
                BackupMessage::RegisterImage { device_name, size, result_channel } => {
                    match client {
                        Some(ref client) => {
                            let res = register_image(
                                client.clone(),
                                crypt_config.clone(),
                                registry.clone(),
                                known_chunks.clone(),
                                device_name,
                                size,
                                chunk_size,
                            ).await;
                            let _ = result_channel.lock().unwrap().send(res);
                        }
                        None => {
                           let _ = result_channel.lock().unwrap().send(Err(format_err!("not connected")));
                        }
                    }
                }
                BackupMessage::CloseImage { dev_id, callback_info } => {
                    match client {
                        Some(ref client) => {
                            handle_async_command(
                                close_image(client.clone(), registry.clone(), dev_id),
                                abort.listen(),
                                callback_info,
                            ).await;
                        }
                        None => {
                            callback_info.send_result(Err(format_err!("not connected")));
                        }
                    }
                }
                BackupMessage::WriteData { dev_id, data, offset, size, callback_info } => {
                    match client {
                        Some(ref client) => {
                            written_bytes2.fetch_add(size, Ordering::SeqCst);
                            handle_async_command(
                                write_data(
                                    client.clone(),
                                    crypt_config.clone(),
                                    registry.clone(),
                                    known_chunks.clone(),
                                    dev_id, data,
                                    offset,
                                    size,
                                    chunk_size,
                                ),
                                abort.listen(),
                                callback_info,
                            ).await;
                        }
                        None => {
                            callback_info.send_result(Err(format_err!("not connected")));
                        }
                    }
                }
                BackupMessage::Finish { callback_info } => {
                    match client {
                        Some(ref client) => {
                            handle_async_command(
                                finish_backup(
                                    client.clone(),
                                    crypt_config.clone(),
                                    registry.clone(),
                                    setup.clone(),
                                ),
                                abort.listen(),
                                callback_info,
                            ).await;
                        }
                        None => {
                            callback_info.send_result(Err(format_err!("not connected")));
                        }
                    }
                }
            }
        }

        println!("worker end loop");
    });

    runtime.shutdown_on_idle();

    let stats = BackupTaskStats { written_bytes: written_bytes.fetch_add(0, Ordering::SeqCst)  };
    Ok(stats)
}
