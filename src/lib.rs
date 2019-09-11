use failure::*;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::mpsc::{channel, Sender, Receiver};

//use futures::{future, Future, Stream};
use tokio::runtime::current_thread::{Runtime, RunError};

//#[macro_use]
use proxmox_backup::client::*;

use chrono::{Utc, TimeZone};


struct BackupTask {
    worker: JoinHandle<Result<(), RunError>>,
    tx: Sender<BackupMessage>,
//    runtime: Runtime,
//    client: Arc<BackupClient>,
}

enum BackupMessage {
    End,
    WriteData {
        data: *const u8,
        size: u64,
        callback: extern "C" fn(*mut libc::c_void),
        callback_data: *mut libc::c_void,
    },
}

unsafe impl std::marker::Send for BackupMessage {} // fixme: ???

impl BackupTask {

    fn new() -> Result<Self, Error> {

        let host = "localhost";
        let user = "root@pam";
        let store = "store2";
        let backup_id = "99";
        let verbose = false;
        
        let backup_time = Utc.timestamp(Utc::now().timestamp(), 0);

        let (tx, rx) = channel(1);
        
        let worker = std::thread::spawn(move ||  {
            backup_worker_task(rx, host)
        });
        
        /*
        let client = HttpClient::new(host, user)?;

        let client = runtime.block_on(
            client.start_backup(store, "vm", backup_id, backup_time, verbose))?;
         */
        
        Ok(BackupTask {
            worker,
            tx,
        })
    }

}

fn backup_worker_task(mut rx: Receiver<BackupMessage>, host: &str) -> Result<(), RunError>  {

    let mut runtime = Runtime::new().unwrap(); // fixme

    runtime.spawn(async move {
        
        while let Some(msg) = rx.recv().await {

            match msg {
                BackupMessage::End => {
                    break;
                }
                BackupMessage::WriteData { data, size, callback, callback_data } => {
                    println!("write {} bytes", size);

                    for i in 0..10 {
                        println!("Delay loop {}", i);
                        tokio::timer::delay(std::time::Instant::now() + std::time::Duration::new(1, 0)).await;
                    }
  
                    // fixme: error handling 
                    callback(callback_data);
                }
            }
        }
    });

    runtime.run()
}

// The C interface

#[repr(C)]
pub struct ProxmoxBackupHandle {}


#[no_mangle]
pub unsafe extern "C" fn proxmox_backup_connect() -> *mut ProxmoxBackupHandle {

    println!("Hello");

    match BackupTask::new() {
        Ok(task) => {
            let tmp = Box::new(task);
            let test = Box::into_raw(tmp);
            test as * mut ProxmoxBackupHandle
        }
        Err(err) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn proxmox_backup_write_data_async(
    handle: *mut ProxmoxBackupHandle,
    data: *const u8,
    size: u64,
    callback: extern "C" fn(*mut libc::c_void),
    callback_data: *mut libc::c_void,
) {

    let msg = BackupMessage::WriteData { data, size , callback, callback_data };

    let task = handle as * mut BackupTask;
    
    let _res = (*task).tx.send(msg); // fixme: log errors
}

#[no_mangle]
pub unsafe extern "C" fn proxmox_backup_disconnect(handle: *mut ProxmoxBackupHandle) {

    println!("diconnect");

    let task = handle as * mut BackupTask;
    let mut task = Box::from_raw(task); // take ownership
   
    let _res = task.tx.send(BackupMessage::End); // fixme: log errors
    
    match task.worker.join() {
        Ok(result) => {
            match result {
                Ok(()) => {
                    println!("worker finished");
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

