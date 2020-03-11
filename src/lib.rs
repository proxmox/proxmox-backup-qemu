use failure::*;
use std::ffi::{CStr, CString};
use std::ptr;
use std::os::raw::{c_uchar, c_char, c_int, c_void};
use std::sync::{Mutex, Condvar};

use proxmox::try_block;
use proxmox_backup::client::BackupRepository;
use proxmox_backup::backup::BackupDir;
use chrono::{DateTime, Utc, TimeZone};

mod capi_types;
use capi_types::*;

mod upload_queue;

mod commands;

mod worker_task;
use worker_task::*;

mod restore;
use restore::*;

pub const PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE: u64 = 1024*1024*4;

/// Free returned error messages
///
/// All calls can return error messages, but they are allocated using
/// the rust standard library. This call moves ownership back to rust
/// and free the allocated memory.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_free_error(ptr: * mut c_char) {
    if !ptr.is_null() {
        unsafe { CString::from_raw(ptr); }
    }
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

#[derive(Clone)]
pub(crate) struct BackupSetup {
    pub host: String,
    pub store: String,
    pub user: String,
    pub chunk_size: u64,
    pub backup_id: String,
    pub backup_time: DateTime<Utc>,
    pub password: Option<String>,
    pub keyfile: Option<std::path::PathBuf>,
    pub key_password: Option<String>,
    pub fingerprint: Option<String>,
}

// helper class to implement synchrounous interface
struct GotResultCondition {
    lock: Mutex<bool>,
    cond: Condvar,
}

impl GotResultCondition {

    pub fn new() -> Self {
        Self {
            lock: Mutex::new(false),
            cond: Condvar::new(),
        }
    }

    /// Create CallbackPointers
    ///
    /// wait() returns If the contained callback is called.
    pub fn callback_info(
        &mut self,
        result: *mut c_int,
        error: *mut *mut c_char,
    ) -> CallbackPointers {
        CallbackPointers {
            callback: Self::wakeup_callback,
            callback_data: (self) as *mut _ as *mut c_void,
            error,
            result: result,
        }
    }

    /// Waits until the callback from callback_info is called.
    pub fn wait(&mut self) {
        let mut done = self.lock.lock().unwrap();
        while !*done {
            done = self.cond.wait(done).unwrap();
        }
    }

    #[no_mangle]
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    extern "C" fn wakeup_callback(
        callback_data: *mut c_void,
    ) {
        let callback_data = unsafe { &mut *( callback_data as * mut GotResultCondition) };
        let mut done = callback_data.lock.lock().unwrap();
        *done = true;
        callback_data.cond.notify_one();
    }
}

/// Create a new instance
///
/// Uses `PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE` if `chunk_size` is zero.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_new(
    repo: *const c_char,
    backup_id: *const c_char,
    backup_time: u64,
    chunk_size: u64,
    password: *const c_char,
    keyfile: *const c_char,
    key_password: *const c_char,
    fingerprint: *const c_char,
    error: * mut * mut c_char,
) -> *mut ProxmoxBackupHandle {

    let task: Result<_, Error> = try_block!({
        let repo = unsafe { CStr::from_ptr(repo).to_str()?.to_owned() };
        let repo: BackupRepository = repo.parse()?;

        let backup_id = unsafe { CStr::from_ptr(backup_id).to_str()?.to_owned() };

        let backup_time = Utc.timestamp(backup_time as i64, 0);

        let password = if password.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(password).to_str()?.to_owned() })
        };

        let keyfile = if keyfile.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(keyfile).to_str().map(std::path::PathBuf::from)? })
        };

        let key_password = if key_password.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(key_password).to_str()?.to_owned() })
        };

        let fingerprint = if fingerprint.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(fingerprint).to_str()?.to_owned() })
        };

        let setup = BackupSetup {
            host: repo.host().to_owned(),
            user: repo.user().to_owned(),
            store: repo.store().to_owned(),
            chunk_size: if chunk_size > 0 { chunk_size } else { PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE },
            backup_id,
            password,
            backup_time,
            keyfile,
            key_password,
            fingerprint,
        };

        BackupTask::new(setup)
    });

    match task {
        Ok(task) => {
            let boxed_task = Box::new(task);
            Box::into_raw(boxed_task) as * mut ProxmoxBackupHandle
        }
        Err(err) => raise_error_null!(error, err),
    }
}

/// Open connection to the backup server (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_connect(
    handle: *mut ProxmoxBackupHandle,
    error: *mut *mut c_char,
) -> c_int {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    let msg = BackupMessage::Connect { callback_info};

    if let Err(_) = task.command_tx.send(msg) {
        let errmsg = CString::new("task already aborted (send command failed)".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    got_result_condition.wait();

    return result;
}

/// Open connection to the backup server
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_connect_async(
    handle: *mut ProxmoxBackupHandle,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: *mut *mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    if let Some(_reason) = &task.aborted {
        callback_info.send_result(Err(format_err!("task already aborted")));
        return;
    }

    let msg = BackupMessage::Connect { callback_info };

    if let Err(err) = task.command_tx.send(msg) {
        eprintln!("proxmox_backup_connect_async send command failed - {}", err);
    }
}

/// Abort a running backup task
///
/// This stops the current backup task. It is still necessary to call
/// proxmox_backup_disconnect() to close the connection and free
/// allocated memory.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_abort(
    handle: *mut ProxmoxBackupHandle,
    reason: *const c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    let reason = unsafe { CStr::from_ptr(reason).to_string_lossy().into_owned() };
    task.aborted = Some(reason);

    let _res = task.command_tx.send(BackupMessage::Abort);
}

/// Register a backup image (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_register_image(
    handle: *mut ProxmoxBackupHandle,
    device_name: *const c_char, // expect utf8 here
    size: u64,
    error: * mut * mut c_char,
) -> c_int {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    let device_name = unsafe { CStr::from_ptr(device_name).to_string_lossy().to_string() };

    let msg = BackupMessage::RegisterImage { device_name, size, callback_info };

    if let Err(_) = task.command_tx.send(msg) {
        let errmsg = CString::new("task already aborted (send command failed)".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    got_result_condition.wait();

    return result;
}

/// Register a backup image
///
/// Create a new image archive on the backup server
/// ('<device_name>.img.fidx'). The returned integer is the dev_id
/// parameter for the proxmox_backup_write_data_async() method.
///
/// Note: This call is currently not async and can block.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_register_image_async(
    handle: *mut ProxmoxBackupHandle,
    device_name: *const c_char, // expect utf8 here
    size: u64,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    if let Some(_reason) = &task.aborted {
        callback_info.send_result(Err(format_err!("task already aborted")));
        return;
    }

    let device_name = unsafe { CStr::from_ptr(device_name).to_string_lossy().to_string() };

    let msg = BackupMessage::RegisterImage { device_name, size, callback_info };

    if let Err(err) = task.command_tx.send(msg) {
        eprintln!("proxmox_backup_register_image_async send command failed - {}", err);
    }
}

/// Add a configuration blob to the backup (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_add_config(
    handle: *mut ProxmoxBackupHandle,
    name: *const c_char, // expect utf8 here
    data: *const u8,
    size: u64,
    error: * mut * mut c_char,
) -> c_int {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    let name = unsafe { CStr::from_ptr(name).to_string_lossy().to_string() };

    let msg = BackupMessage::AddConfig {
        name,
        data: DataPointer(data),
        size,
        callback_info,
    };

    if let Err(_) = task.command_tx.send(msg) {
        let errmsg = CString::new("task already aborted (send command failed)".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    got_result_condition.wait();

    return result;
}

/// Add a configuration blob to the backup
///
/// Create and upload a data blob "<name>.blob".
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_add_config_async(
    handle: *mut ProxmoxBackupHandle,
    name: *const c_char, // expect utf8 here
    data: *const u8,
    size: u64,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    if let Some(_reason) = &task.aborted {
        callback_info.send_result(Err(format_err!("task already aborted")));
        return;
    }

    let name = unsafe { CStr::from_ptr(name).to_string_lossy().to_string() };

    let msg = BackupMessage::AddConfig {
        name,
        data: DataPointer(data),
        size,
        callback_info,
    };

    if let Err(err) = task.command_tx.send(msg) {
        eprintln!("proxmox_backup_add_config_async send command failed - {}", err);
    }
}

/// Write data to into a registered image (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_write_data(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    data: *const u8,
    offset: u64,
    size: u64,
    error: * mut * mut c_char,
) -> c_int {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    let msg = BackupMessage::WriteData {
        dev_id,
        data: DataPointer(data),
        offset,
        size,
        callback_info,
    };

    if let Err(_) = task.command_tx.send(msg) {
        let errmsg = CString::new("task already aborted (send command failed)".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    got_result_condition.wait();

    return result;
}

/// Write data to into a registered image
///
/// Upload a chunk of data for the <dev_id> image.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_write_data_async(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    data: *const u8,
    offset: u64,
    size: u64,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    if let Some(_reason) = &task.aborted {
        callback_info.send_result(Err(format_err!("task already aborted")));
        return;
    }

    let msg = BackupMessage::WriteData {
        dev_id,
        data: DataPointer(data),
        offset,
        size,
        callback_info,
    };

    if let Err(err) = task.command_tx.send(msg) {
        eprintln!("proxmox_backup_write_data_async send command failed - {}", err);
    }
}

/// Close a registered image (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_close_image(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    error: * mut * mut c_char,
) -> c_int {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    let msg = BackupMessage::CloseImage { dev_id, callback_info };

    if let Err(_) = task.command_tx.send(msg) {
        let errmsg = CString::new("task already aborted (send command failed)".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    got_result_condition.wait();

    return result;
}

/// Close a registered image
///
/// Mark the image as closed. Further writes are not possible.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_close_image_async(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    if let Some(_reason) = &task.aborted {
        callback_info.send_result(Err(format_err!("task already aborted")));
        return;
    }

    let msg = BackupMessage::CloseImage { dev_id, callback_info };

    if let Err(err) = task.command_tx.send(msg) {
        eprintln!("proxmox_backup_close_image_async send command failed - {}", err);
    }
}

/// Finish the backup (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_finish(
    handle: *mut ProxmoxBackupHandle,
    error: * mut * mut c_char,
) -> c_int {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    let msg = BackupMessage::Finish { callback_info };

    if let Err(_) = task.command_tx.send(msg) {
        let errmsg = CString::new("task already aborted (send command failed)".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        return -1;
    }

    got_result_condition.wait();

    return result;
}

/// Finish the backup
///
/// Finish the backup by creating and uploading the backup manifest.
/// All registered images have to be closed before calling this.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_finish_async(
    handle: *mut ProxmoxBackupHandle,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    if let Some(_reason) = &task.aborted {
        callback_info.send_result(Err(format_err!("task already aborted")));
        return;
    }

    let msg = BackupMessage::Finish { callback_info };

    if let Err(err) = task.command_tx.send(msg) {
        eprintln!("proxmox_backup_finish_async send command failed - {}", err);
    }
}

// fixme: should be async
/// Disconnect and free allocated memory
///
/// The handle becomes invalid after this call.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_disconnect(handle: *mut ProxmoxBackupHandle) {

    let task = handle as * mut BackupTask;
    let task = unsafe { Box::from_raw(task) }; // take ownership

    if let Err(err) = task.command_tx.send(BackupMessage::End) {
        eprintln!("proxmox_backup_disconnect send command failed - {}", err);
    }

    match task.worker.join() {
        Ok(result) => {
            if let Err(err) = result {
                eprintln!("worker finished with error: {:?}", err);
            }
        }
        Err(err) => {
            eprintln!("worker paniced with error: {:?}", err);
        }
    }

    //drop(task);
}


/// Simple interface to restore images
///
/// Connect the the backup server.
///
/// Note: This implementation is not async
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_connect(
    repo: *const c_char,
    snapshot: *const c_char,
    password: *const c_char,
    keyfile: *const c_char,
    key_password: *const c_char,
    fingerprint: *const c_char,
    error: * mut * mut c_char,
) -> *mut ProxmoxRestoreHandle {

    let result: Result<_, Error> = try_block!({
        let repo = unsafe { CStr::from_ptr(repo).to_str()?.to_owned() };
        let repo: BackupRepository = repo.parse()?;

        let snapshot = unsafe { CStr::from_ptr(snapshot).to_string_lossy().into_owned() };
        let snapshot = BackupDir::parse(&snapshot)?;

        let backup_type = snapshot.group().backup_type();
        let backup_id = snapshot.group().backup_id().to_owned();
        let backup_time = snapshot.backup_time();

        if backup_type != "vm" {
            bail!("wrong backup type ({} != vm)", backup_type);
        }

        let password = if password.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(password).to_str()?.to_owned() })
        };

        let keyfile = if keyfile.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(keyfile).to_str().map(std::path::PathBuf::from)? })
        };

        let key_password = if key_password.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(key_password).to_str()?.to_owned() })
        };

        let fingerprint = if fingerprint.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(fingerprint).to_str()?.to_owned() })
        };

        let setup = BackupSetup {
            host: repo.host().to_owned(),
            user: repo.user().to_owned(),
            store: repo.store().to_owned(),
            chunk_size: PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE, // not used by restore
            backup_id,
            password,
            backup_time,
            keyfile,
            key_password,
            fingerprint,
        };

        ProxmoxRestore::new(setup)
    });

    match result {
        Ok(conn) => {
            let boxed_task = Box::new(conn);
            Box::into_raw(boxed_task) as * mut ProxmoxRestoreHandle
        }
        Err(err) => raise_error_null!(error, err),
    }
}

/// Disconnect and free allocated memory
///
/// The handle becomes invalid after this call.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_disconnect(handle: *mut ProxmoxRestoreHandle) {

    let conn = handle as * mut ProxmoxRestore;
    unsafe { Box::from_raw(conn) }; //drop(conn)
}

/// Restore an image
///
/// Image data is downloaded and sequentially dumped to the callback.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_image(
    handle: *mut ProxmoxRestoreHandle,
    archive_name: *const c_char, // expect full name here, i.e. "name.img.fidx"
    callback: extern "C" fn(*mut c_void, u64, *const c_uchar, u64) -> c_int,
    callback_data: *mut c_void,
    error: * mut * mut c_char,
    verbose: bool,
) -> c_int {

    let conn = unsafe { &mut *(handle as * mut ProxmoxRestore) };

    let result: Result<_, Error> = try_block!({

        let archive_name = unsafe { CStr::from_ptr(archive_name).to_str()?.to_owned() };

        let write_data_callback = move |offset: u64, data: &[u8]| {
            callback(callback_data, offset, data.as_ptr(), data.len() as u64)
        };

        let write_zero_callback = move |offset: u64, len: u64| {
            callback(callback_data, offset, std::ptr::null(), len)
        };

        conn.restore(archive_name, write_data_callback, write_zero_callback, verbose)?;

        Ok(())
    });

    if let Err(err) = result {
        raise_error_int!(error, err);
    };

    0
}
