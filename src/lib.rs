use failure::*;
use std::thread::JoinHandle;
use std::sync::Arc;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::ffi::{CStr, CString};
use std::ptr;
use std::os::raw::{c_char, c_int, c_void};

//use futures::{future, Future, Stream};
use tokio::runtime::current_thread::Runtime;

//#[macro_use]
use proxmox_backup::client::*;

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

enum BackupMessage {
    End,
    WriteData {
        dev_id: u8,
        data: *const u8,
        size: u64,
        callback: extern "C" fn(*mut c_void),
        callback_data: *mut c_void,
    },
}

unsafe impl std::marker::Send for BackupMessage {} // fixme: ???

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

fn backup_worker_task(
    repo: BackupRepository,
    connect_tx: Sender<Result<(), Error>>,
    command_rx: Receiver<BackupMessage>,
) -> Result<BackupStats, Error>  {

    let mut runtime = match Runtime::new() {
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

    let mut stats = BackupStats { written_bytes: 0 };

    loop {
        let msg = command_rx.recv()?;

        match msg {
            BackupMessage::End => {
                println!("worker got end mesage");
                break;
            }
            BackupMessage::WriteData { dev_id, data, size, callback, callback_data } => {
                stats.written_bytes += size;
                println!("dev {}: write {} bytes ({})", dev_id, size, stats.written_bytes);

                runtime.block_on(async move {
                    //println!("Delay test");
                    //tokio::timer::delay(std::time::Instant::now() + std::time::Duration::new(1, 0)).await;
                    //println!("Delay end");

                    // fixme: error handling
                    callback(callback_data);
                });
            }
        }
    }

    println!("worker end loop");

    //    runtime.run()

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
        password: "".to_owned(),
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
pub extern "C" fn proxmox_backup_write_data_async(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    data: *const u8,
    size: u64,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
) {

    let msg = BackupMessage::WriteData { dev_id, data, size , callback, callback_data };

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
