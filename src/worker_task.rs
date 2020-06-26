use anyhow::{bail, format_err, Error};
use std::collections::HashSet;
use std::thread::JoinHandle;
use std::sync::{Mutex, Arc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::os::raw::c_int;

use futures::future::{Future, Either, FutureExt};

use proxmox_backup::tools::BroadcastFuture;
use proxmox_backup::backup::{CryptConfig, load_and_decrypt_key};
use proxmox_backup::client::{HttpClient, HttpClientOptions, BackupWriter};

use super::BackupSetup;
use crate::capi_types::*;
use crate::commands::*;

pub(crate) struct BackupTask {
    pub worker: JoinHandle<Result<BackupTaskStats, Error>>,
    pub command_tx: Sender<BackupMessage>,
    pub aborted: Option<String>,  // set on abort, conatins abort reason
}

#[derive(Debug)]
pub(crate) struct BackupTaskStats { // fixme: do we really need this?
    written_bytes: u64,
}

impl BackupTask {

    pub fn new(setup: BackupSetup) -> Result<Self, Error> {

        let crypt_config = match setup.keyfile {
            None => None,
            Some(ref path) => {
                let (key, _) = load_and_decrypt_key(path, & || {
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

        let worker = std::thread::Builder::new().name(String::from("proxmox-backup-qemu-main-worker")).spawn(move ||  {
            backup_worker_task(setup, crypt_config, connect_tx, command_rx)
        })?;

        connect_rx.recv()??; // sync

        Ok(BackupTask {
            worker,
            command_tx,
            aborted: None,
        })
    }
}

fn handle_async_command<F: 'static + Send + Future<Output=Result<c_int, Error>>>(
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
    builder.threaded_scheduler();
    builder.enable_all();
    builder.max_threads(6);
    builder.core_threads(4);
    builder.thread_name("proxmox-backup-qemu-worker");

    let mut runtime = match builder.build() {
        Ok(runtime) => runtime,
        Err(err) =>  {
            connect_tx.send(Err(format_err!("create runtime failed: {}", err)))?;
            bail!("create runtime failed");
        }
    };

    connect_tx.send(Ok(()))?;
    drop(connect_tx); // no longer needed

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
    let manifest = Arc::new(Mutex::new(None));

    let chunk_size = setup.chunk_size;

    let client = Arc::new(Mutex::new(None));

    runtime.block_on(async move  {

        let registry = Arc::new(Mutex::new(ImageRegistry::new()));

        loop {
            // Note: command_rx.recv() may block one thread, because there are
            // still enough threads to do the work
            let msg = command_rx.recv();
            if msg.is_err() {
                // sender closed channel, try to abort and then end the loop
                let _ = abort_tx.send(()).await;
                break;
            };

            let msg = msg.unwrap();

            match msg {
                BackupMessage::Connect { callback_info } => {
                    let setup = setup.clone();
                    let client = client.clone();
                    let crypt_config = crypt_config.clone();
                    let manifest = manifest.clone();

                    let command_future = async move {
                        let options = HttpClientOptions::new()
                            .fingerprint(setup.fingerprint.clone())
                            .password(setup.password.clone());

                        let http = HttpClient::new(&setup.host, &setup.user, options)?;
                        let writer = BackupWriter::start(http, crypt_config.clone(), &setup.store, "vm", &setup.backup_id, setup.backup_time, false).await?;

                        let last_manifest = writer.download_previous_manifest().await;
                        let mut manifest_guard = manifest.lock().unwrap();
                        *manifest_guard = match last_manifest {
                            Ok(last_manifest) => Some(Arc::new(last_manifest)),
                            Err(_) => None
                        };

                        let mut client_guard = client.lock().unwrap();
                        *client_guard = Some(writer);
                        Ok(0)
                    };

                    tokio::spawn(handle_async_command(command_future, abort.listen(), callback_info));
                }
                BackupMessage::Abort => {
                    let res = abort_tx.send(()).await;
                    if let Err(_err) = res  {
                        eprintln!("sending abort failed");
                    }
                }
                BackupMessage::End => {
                    break;
                }
                BackupMessage::AddConfig { name, data, size, callback_info } => {
                    let client = (*(client.lock().unwrap())).clone();
                    match client {
                        Some(client) => {
                            let command_future = add_config(
                                client,
                                registry.clone(),
                                name,
                                data,
                                size,
                            );
                            tokio::spawn(handle_async_command(command_future, abort.listen(), callback_info));
                        }
                        None => {
                            callback_info.send_result(Err(format_err!("not connected")));
                        }
                    }
                }
                BackupMessage::RegisterImage { device_name, size, incremental, callback_info } => {
                    let client = (*(client.lock().unwrap())).clone();
                    let manifest = (*(manifest.lock().unwrap())).clone();

                    match client {
                        Some(client) => {
                            let command_future = register_image(
                                client,
                                crypt_config.clone(),
                                manifest,
                                registry.clone(),
                                known_chunks.clone(),
                                device_name,
                                size,
                                chunk_size,
                                incremental,
                            );
                            tokio::spawn(handle_async_command(command_future, abort.listen(), callback_info));
                        }
                        None => {
                            callback_info.send_result(Err(format_err!("not connected")));
                        }
                    }
                }
                BackupMessage::CloseImage { dev_id, callback_info } => {
                    let client = (*(client.lock().unwrap())).clone();
                     match client {
                         Some(client) => {
                             let command_future = close_image(client, registry.clone(), dev_id);
                             tokio::spawn(handle_async_command(command_future, abort.listen(), callback_info));
                         }
                         None => {
                             callback_info.send_result(Err(format_err!("not connected")));
                         }
                     }
                }
                BackupMessage::WriteData { dev_id, data, offset, size, callback_info } => {
                    let client = (*(client.lock().unwrap())).clone();
                    match client {
                        Some(client) => {
                            written_bytes2.fetch_add(size, Ordering::SeqCst);

                            let command_future = write_data(
                                client,
                                crypt_config.clone(),
                                registry.clone(),
                                known_chunks.clone(),
                                dev_id, data,
                                offset,
                                size,
                                chunk_size,
                            );
                            tokio::spawn(handle_async_command(command_future, abort.listen(), callback_info));
                        }
                        None => {
                            callback_info.send_result(Err(format_err!("not connected")));
                        }
                    }
                }
                BackupMessage::Finish { callback_info } => {
                    let client = (*(client.lock().unwrap())).clone();
                    match client {
                        Some(client) => {
                            let command_future = finish_backup(
                                client,
                                registry.clone(),
                                setup.clone(),
                            );
                            tokio::spawn(handle_async_command(command_future, abort.listen(), callback_info));
                        }
                        None => {
                            callback_info.send_result(Err(format_err!("not connected")));
                        }
                    }
                }
            }
        }
        //println!("worker end loop");
    });

    let stats = BackupTaskStats { written_bytes: written_bytes.fetch_add(0, Ordering::SeqCst)  };
    Ok(stats)
}
