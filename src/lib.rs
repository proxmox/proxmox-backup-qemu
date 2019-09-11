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

 
                    // do something async
                    let res = std::panic::catch_unwind(|| {

                        
                    });
  
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
pub struct BackupHandle {
    task: Box<BackupTask>,
}


#[no_mangle]
pub unsafe extern "C" fn connect() -> *mut BackupHandle {

    println!("Hello");

    match BackupTask::new() {
        Ok(task) => {
            let tmp = Box::new(task);
            let test = Box::into_raw(tmp);
            test as * mut BackupHandle
        }
        Err(err) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn write_data_async(
    handle: *mut BackupHandle,
    data: *const u8,
    size: u64,
    callback: extern "C" fn(*mut libc::c_void),
    callback_data: *mut libc::c_void,
) {

    let msg = BackupMessage::WriteData { data, size , callback, callback_data };
    
    let _res = (*handle).task.tx.send(msg); // fixme: log errors
}

#[no_mangle]
pub unsafe extern "C" fn disconnect(handle: *mut BackupHandle) {

    println!("diconnect");

    let mut handle = Box::from_raw(handle);
   
    let _res = handle.task.tx.send(BackupMessage::End); // fixme: log errors
    
    match handle.task.worker.join() {
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
    
    //drop(handle);
}

