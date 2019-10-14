use failure::*;
use std::sync::{Mutex, Arc};
use std::sync::mpsc::channel;
use std::ffi::{CStr, CString};
use std::ptr;
use std::os::raw::{c_uchar, c_char, c_int, c_void};

use proxmox::tools::try_block;
use proxmox_backup::backup::*;
use proxmox_backup::client::BackupRepository;

use chrono::{Utc, TimeZone};

mod capi_types;
use capi_types::*;

mod upload_queue;

mod commands;
use commands::*;

mod worker_task;
use worker_task::*;

mod restore;
use restore::*;

// The C interface

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
pub extern "C" fn proxmox_backup_connect(
    repo: *const c_char,
    backup_id: *const c_char,
    backup_time: u64,
    password: *const c_char,
    error: * mut * mut c_char,
) -> *mut ProxmoxBackupHandle {

    println!("Hello");

    let repo = unsafe { CStr::from_ptr(repo).to_string_lossy().into_owned() };
    let repo: BackupRepository = match repo.parse() {
        Ok(repo) => repo,
        Err(err) => raise_error_null!(error, err),
    };
    let backup_id = unsafe { CStr::from_ptr(backup_id).to_string_lossy().into_owned() };

    let backup_time = Utc.timestamp(backup_time as i64, 0);

    let password = unsafe { CStr::from_ptr(password).to_owned() };

    let crypt_config: Option<Arc<CryptConfig>> = None; //fixme:

    let setup = BackupSetup {
        host: repo.host().to_owned(),
        user: repo.user().to_owned(),
        store: repo.store().to_owned(),
        chunk_size: 4*1024*1024,
        backup_id,
        password,
        backup_time,
        crypt_config,
    };

    match BackupTask::new(setup) {
        Ok(task) => {
            let boxed_task = Box::new(task);
            Box::into_raw(boxed_task) as * mut ProxmoxBackupHandle
        }
        Err(err) => raise_error_null!(error, err),
    }
}

#[no_mangle]
pub extern "C" fn proxmox_backup_abort(
    handle: *mut ProxmoxBackupHandle,
    reason: *const c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    let reason = unsafe { CStr::from_ptr(reason).to_string_lossy().into_owned() };
    task.aborted = Some(reason);

    println!("send abort");
    let _res = task.command_tx.send(BackupMessage::Abort);
}

#[no_mangle]
pub extern "C" fn proxmox_backup_register_image(
    handle: *mut ProxmoxBackupHandle,
    device_name: *const c_char, // expect utf8 here
    size: u64,
    error: * mut * mut c_char,
) -> c_int {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        raise_error_int!(error, "task already aborted");
    }

    let device_name = unsafe { CStr::from_ptr(device_name).to_string_lossy().to_string() };

    let (result_sender, result_receiver) = channel();

    let msg = BackupMessage::RegisterImage {
        device_name,
        size,
        result_channel: Arc::new(Mutex::new(result_sender)),
    };

    println!("register_image_async start");
    let _res = task.command_tx.send(msg); // fixme: log errors
    println!("register_image_async send end");

    match result_receiver.recv() {
        Err(err) => raise_error_int!(error, format!("channel recv error: {}", err)),
        Ok(Err(err)) => raise_error_int!(error, err.to_string()),
        Ok(Ok(result)) => result as c_int,
    }
}

#[no_mangle]
pub extern "C" fn proxmox_backup_add_config(
    handle: *mut ProxmoxBackupHandle,
    name: *const c_char, // expect utf8 here
    data: *const u8,
    size: u64,
    error: * mut * mut c_char,
) -> c_int {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        raise_error_int!(error, "task already aborted");
    }

    let name = unsafe { CStr::from_ptr(name).to_string_lossy().to_string() };

    let (result_sender, result_receiver) = channel();

    let msg = BackupMessage::AddConfig {
        name,
        data: DataPointer(data),
        size,
        result_channel: Arc::new(Mutex::new(result_sender)),
    };

    println!("add config start");
    let _res = task.command_tx.send(msg); // fixme: log errors
    println!("add config send end");

    match result_receiver.recv() {
        Err(err) => raise_error_int!(error, format!("channel recv error: {}", err)),
        Ok(Err(err)) => raise_error_int!(error, err.to_string()),
        Ok(Ok(())) => 0,
    }
}

#[no_mangle]
pub extern "C" fn proxmox_backup_write_data_async(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    data: *const u8,
    offset: u64,
    size: u64,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        callback(callback_data);
        return;
    }

    let msg = BackupMessage::WriteData {
        dev_id,
        data: DataPointer(data),
        offset,
        size,
        callback_info: CallbackPointers { callback, callback_data, error },
    };

    println!("write_data_async start");
    let _res = task.command_tx.send(msg); // fixme: log errors
    println!("write_data_async end");
}

#[no_mangle]
pub extern "C" fn proxmox_backup_close_image_async(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        callback(callback_data);
        return;
    }

    let msg = BackupMessage::CloseImage {
        dev_id,
        callback_info: CallbackPointers { callback, callback_data, error },
    };

    println!("close_image_async start");
    let _res = task.command_tx.send(msg); // fixme: log errors
    println!("close_image_async end");
}

#[no_mangle]
pub extern "C" fn proxmox_backup_finish_async(
    handle: *mut ProxmoxBackupHandle,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        callback(callback_data);
        return;
    }

    let msg = BackupMessage::Finish {
        callback_info: CallbackPointers { callback, callback_data, error },
    };

    println!("finish_async start");
    let _res = task.command_tx.send(msg); // fixme: log errors
    println!("finish_async end");
}

// fixme: should be async
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


/// Simple interface to restore images
///
/// Note: This implementation is not async
#[no_mangle]
pub extern "C" fn proxmox_backup_restore(
    repo: *const c_char,
    snapshot: *const c_char,
    archive_name: *const c_char, // expect full name here, i.e. "name.img.fidx"
    keyfile: *const c_char,
    callback: extern "C" fn(*mut c_void, u64, *const c_uchar, u64) -> c_int,
    callback_data: *mut c_void,
    error: * mut * mut c_char,
    verbose: bool,
) -> c_int {

    let result: Result<_, Error> = try_block!({
        let repo = unsafe { CStr::from_ptr(repo).to_str()?.to_owned() };
        let repo: BackupRepository = repo.parse()?;

        let archive_name = unsafe { CStr::from_ptr(archive_name).to_str()?.to_owned() };
        let keyfile = if keyfile == std::ptr::null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(keyfile).to_str().map(|p| std::path::PathBuf::from(p))? })
        };

        let snapshot = unsafe { CStr::from_ptr(snapshot).to_string_lossy().into_owned() };
        let snapshot = BackupDir::parse(&snapshot)?;

        let write_data_callback = move |offset: u64, data: &[u8]| {
            callback(callback_data, offset, data.as_ptr(), data.len() as u64)
        };
        let write_zero_callback = move |offset: u64, len: u64| {
            callback(callback_data, offset, std::ptr::null(), len)
        };

        restore(repo, snapshot, archive_name, keyfile, write_data_callback, write_zero_callback, verbose)?;

        Ok(())
    });

    if let Err(err) = result {
        raise_error_int!(error, err);
    };

    return 0;
}
