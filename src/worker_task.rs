use failure::*;
use std::collections::HashSet;
use std::thread::JoinHandle;
use std::sync::{Mutex, Arc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::ffi::CString;
use std::ptr;

use futures::future::{Future, Either, FutureExt};
use tokio::runtime::Runtime;

use proxmox_backup::client::*;
use proxmox_backup::tools::BroadcastFuture;


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

        let (connect_tx, connect_rx) = channel(); // sync initial server connect

        let (command_tx, command_rx) = channel();

        let worker = std::thread::spawn(move ||  {
            backup_worker_task(setup, connect_tx, command_rx)
        });

        connect_rx.recv().unwrap()?;

        Ok(BackupTask {
            worker,
            command_tx,
            aborted: None,
        })
    }
}

fn connect(runtime: &mut Runtime, setup: &BackupSetup) -> Result<Arc<BackupClient>, Error> {
    let password = setup.password.to_str()?.to_owned();
    let client = HttpClient::new(&setup.host, &setup.user, Some(password))?;

    let client = runtime.block_on(
        client.start_backup(&setup.store, "vm", &setup.backup_id, setup.backup_time, false))?;

    Ok(client)
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
                    match result {
                        Ok(_) => {
                            println!("command sucessful");
                            unsafe { *(callback_info.error) = ptr::null_mut(); }
                            (callback_info.callback)(callback_info.callback_data);
                        }
                        Err(err) => {
                            println!("command error {}", err);
                            let errmsg = CString::new(format!("command error: {}", err)).unwrap();
                            unsafe { *(callback_info.error) = errmsg.into_raw(); }
                            (callback_info.callback)(callback_info.callback_data);
                        }
                    }
                }
                Either::Right(_) => { // aborted
                    println!("command aborted");
                    let errmsg = CString::new("copmmand aborted".to_string()).unwrap();
                    unsafe { *(callback_info.error) = errmsg.into_raw(); }
                    (callback_info.callback)(callback_info.callback_data);
                }
            }
        })
}

fn backup_worker_task(
    setup: BackupSetup,
    connect_tx: Sender<Result<(), Error>>,
    command_rx: Receiver<BackupMessage>,
) -> Result<BackupTaskStats, Error>  {

    let mut builder = tokio::runtime::Builder::new();

    builder.blocking_threads(1);
    builder.core_threads(4);
    builder.name_prefix("proxmox-backup-qemu-");

    let mut runtime = match builder.build() {
        Ok(runtime) => runtime,
        Err(err) =>  {
            connect_tx.send(Err(format_err!("create runtime failed: {}", err))).unwrap();
            bail!("create runtime failed");
        }
    };

    let client = match connect(&mut runtime, &setup) {
        Ok(client) => {
            connect_tx.send(Ok(())).unwrap();
            client
        }
        Err(err) => {
            connect_tx.send(Err(err)).unwrap();
            bail!("connection failed");
        }
    };

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
    let crypt_config = setup.crypt_config.clone();
    let chunk_size = setup.chunk_size;

    runtime.spawn(async move  {

        let registry = Arc::new(Mutex::new(ImageRegistry::new()));

        loop {
            let msg = command_rx.recv().unwrap(); // todo: should be blocking

            match msg {
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
                BackupMessage::RegisterImage { device_name, size, result_channel } => {
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
                BackupMessage::CloseImage { dev_id, callback_info } => {
                    handle_async_command(
                        close_image(client.clone(), registry.clone(), dev_id),
                        abort.listen(),
                        callback_info,
                    ).await;
                }
                BackupMessage::WriteData { dev_id, data, offset, size, callback_info } => {
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
                BackupMessage::Finish { callback_info } => {
                    handle_async_command(
                        finish_backup(
                            client.clone(),
                            registry.clone(),
                            setup.clone(),
                        ),
                        abort.listen(),
                        callback_info,
                    ).await;
                }
            }
        }

        println!("worker end loop");
    });

    runtime.shutdown_on_idle();

    let stats = BackupTaskStats { written_bytes: written_bytes.fetch_add(0, Ordering::SeqCst)  };
    Ok(stats)
}
