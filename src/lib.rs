use failure::*;
use std::thread::JoinHandle;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::ffi::CString;
use std::ptr;
use std::os::raw::{c_char, c_void};

//use futures::{future, Future, Stream};
use futures::future::{self, Future, Either, FutureExt};
use tokio::runtime::Runtime;

//#[macro_use]
use proxmox_backup::client::*;
use proxmox_backup::tools::BroadcastFuture;

use chrono::{Utc, TimeZone, DateTime};

struct BackupRepository {
    host: String,
    store: String,
    user: String,
    backup_id: String,
    backup_time: DateTime<Utc>,
    password: String,
}

struct BackupTask {
    worker: JoinHandle<Result<BackupStats, Error>>,
    command_tx: Sender<BackupMessage>,
}

#[derive(Debug)]
struct BackupStats {
    written_bytes: u64,
}

struct CallbackPointers {
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    error: * mut *mut c_char,
}
unsafe impl std::marker::Send for CallbackPointers {}

struct DataPointer (*const u8);
unsafe impl std::marker::Send for DataPointer {}

enum BackupMessage {
    End,
    Abort,
    WriteData {
        dev_id: u8,
        data: DataPointer,
        size: u64,
        callback_info: CallbackPointers,
    },
}

async fn write_data(
    dev_id: u8,
    _data: DataPointer,
    size: u64,
) -> Result<(), Error> {

    println!("dev {}: write {}", dev_id, size);

    //println!("Delay test");
    //tokio::timer::delay(std::time::Instant::now() + std::time::Duration::new(2, 0)).await;
    //println!("Delay end");

    Ok(())
}

impl BackupTask {

    fn new(repo: BackupRepository) -> Result<Self, Error> {

        let (connect_tx, connect_rx) = channel(); // sync initial server connect

        let (command_tx, command_rx) = channel();

        let worker = std::thread::spawn(move ||  {
            backup_worker_task(repo, connect_tx, command_rx)
        });

        connect_rx.recv().unwrap()?;

        Ok(BackupTask {
            worker,
            command_tx,
        })
    }
}

fn connect(runtime: &mut Runtime, repo: &BackupRepository) -> Result<Arc<BackupClient>, Error> {
    let client = HttpClient::new(&repo.host, &repo.user, Some(repo.password.clone()))?;

    let client = runtime.block_on(
        client.start_backup(&repo.store, "vm", &repo.backup_id, repo.backup_time, false))?;

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
    repo: BackupRepository,
    connect_tx: Sender<Result<(), Error>>,
    command_rx: Receiver<BackupMessage>,
) -> Result<BackupStats, Error>  {

    let mut builder = tokio::runtime::Builder::new();
    
    builder.blocking_threads(1);
    builder.core_threads(4);
    builder.name_prefix("proxmox-backup-qemu-");

    let mut runtime = match builder.build() {
        Ok(runtime) => runtime,
        Err(err) =>  {
            connect_tx.send(Err(format_err!("create runtime failed: {}", err))).unwrap();
            bail!("create runtiome failed");
        }
    };

    let client = match connect(&mut runtime, &repo) {
        Ok(client) => {
            connect_tx.send(Ok(())).unwrap();
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

    runtime.spawn(async move  {

        loop {
            let msg = command_rx.recv().unwrap();

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
                BackupMessage::WriteData { dev_id, data, size, callback_info } => {
                    written_bytes2.fetch_add(size, Ordering::SeqCst);

                    handle_async_command(
                        write_data(dev_id, data, size),
                        abort.listen(),
                        callback_info,
                    ).await;
                }
            }
        }

        println!("worker end loop");
    });

    runtime.shutdown_on_idle();

    let stats = BackupStats { written_bytes: written_bytes.fetch_add(0,Ordering::SeqCst)  };
    Ok(stats)
}

// The C interface

#[repr(C)]
pub struct ProxmoxBackupHandle;

#[no_mangle]
pub extern "C" fn proxmox_backup_free_error(ptr: * mut c_char) {
    unsafe { CString::from_raw(ptr); }
}

macro_rules! raise_error_null {
    ($error:ident, $err:expr) => {{
        let errmsg = CString::new($err.to_string()).unwrap(); // fixme
        unsafe { *$error =  errmsg.into_raw(); }
        return ptr::null_mut();
    }}
}

macro_rules! raise_error_int {
    ($error:ident, $err:expr) => {{
        let errmsg = CString::new($err.to_string()).unwrap(); // fixme
        unsafe { *$error =  errmsg.into_raw(); }
        return -1 as c_int;
    }}
}

#[no_mangle]
pub extern "C" fn proxmox_backup_connect(error: * mut * mut c_char) -> *mut ProxmoxBackupHandle {

    println!("Hello");

    let backup_time = Utc.timestamp(Utc::now().timestamp(), 0);

    let repo = BackupRepository {
        host: "localhost".to_owned(),
        user: "root@pam".to_owned(),
        store: "store2".to_owned(),
        backup_id: "99".to_owned(),
        password: "12345".to_owned(),
        backup_time,
    };

    match BackupTask::new(repo) {
        Ok(task) => {
            let tmp = Box::new(task);
            let test = Box::into_raw(tmp);
            test as * mut ProxmoxBackupHandle
        }
        Err(err) => raise_error_null!(error, err),
    }
}

#[no_mangle]
pub extern "C" fn proxmox_backup_abort(
    handle: *mut ProxmoxBackupHandle,
) {
    let task = handle as * mut BackupTask;

    println!("send abort");
    let _res = unsafe  { (*task).command_tx.send(BackupMessage::Abort) };
}

#[no_mangle]
pub extern "C" fn proxmox_backup_write_data_async(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    data: *const u8,
    size: u64,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    error: * mut * mut c_char,
) {

    let msg = BackupMessage::WriteData {
        dev_id,
        data: DataPointer(data),
        size,
        callback_info: CallbackPointers { callback, callback_data, error },
    };

    let task = handle as * mut BackupTask;

    println!("write_data_async start");
    let _res = unsafe { (*task).command_tx.send(msg) }; // fixme: log errors
    println!("write_data_async end");
}

#[no_mangle]
pub extern "C" fn proxmox_backup_disconnect(handle: *mut ProxmoxBackupHandle) {

    println!("diconnect");

    let task = handle as * mut BackupTask;
    let task = unsafe { Box::from_raw(task) }; // take ownership

    println!("send end");
    let _res = task.command_tx.send(BackupMessage::End); // fixme: log errors

    println!("try join");
    match task.worker.join() {
        Ok(result) => {
            match result {
                Ok(stats) => {
                    println!("worker finished {:?}", stats);
                }
                Err(err) => {
                    println!("worker finished with error: {:?}", err);
                }
            }
        }
        Err(err) => {
           println!("worker paniced with error: {:?}", err);
        }
    }

    //drop(task);
}
