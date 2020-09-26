#![warn(clippy::clone_on_ref_ptr)]

use anyhow::{format_err, Error};
use std::ffi::CString;
use std::ptr;
use std::os::raw::{c_uchar, c_char, c_int, c_void, c_long};
use std::sync::{Arc, Mutex, Condvar};

use proxmox::try_block;
use proxmox_backup::api2::types::Userid;
use proxmox_backup::backup::{CryptMode, BackupDir};
use proxmox_backup::client::BackupRepository;

mod capi_types;
use capi_types::*;

mod registry;
mod upload_queue;
mod commands;

mod backup;
use backup::*;

mod restore;
use restore::*;

mod tools;

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

// Note: UTF8 Strings may contain 0 bytes.
fn convert_error_to_cstring(err: String) -> CString {
    match CString::new(err) {
        Ok(msg) => msg,
        Err(err) => {
            eprintln!("got error containung 0 bytes: {}", err);
            CString::new("failed to convert error message containing 0 bytes").unwrap()
        },
    }
}

macro_rules! raise_error_null {
    ($error:ident, $err:expr) => {{
        let errmsg = convert_error_to_cstring($err.to_string());
        unsafe { *$error =  errmsg.into_raw(); }
        return ptr::null_mut();
    }}
}

macro_rules! raise_error_int {
    ($error:ident, $err:expr) => {{
        let errmsg = convert_error_to_cstring($err.to_string());
        unsafe { *$error =  errmsg.into_raw(); }
        return -1;
    }}
}

macro_rules! param_not_null {
    ($name:ident, $callback_info:expr) => {{
        if $name.is_null() {
            let result = Err(format_err!("{} may not be NULL", stringify!($name)));
            $callback_info.send_result(result);
            return;
        }
    }}
}

/// Returns the text presentation (relative path) for a backup snapshot
///
/// The resturned value is allocated with strdup(), and can be freed
/// with free().
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_snapshot_string(
    backup_type: *const c_char,
    backup_id: *const c_char,
    backup_time: i64,
    error: * mut * mut c_char,
) -> *const c_char {

    let snapshot: Result<CString, Error> = try_block!({
        let backup_type: String = tools::utf8_c_string_lossy(backup_type)
            .ok_or_else(|| format_err!("backup_type must not be NULL"))?;
        let backup_id: String = tools::utf8_c_string_lossy(backup_id)
            .ok_or_else(|| format_err!("backup_id must not be NULL"))?;

        let snapshot = BackupDir::new(backup_type, backup_id, backup_time)?;

        Ok(CString::new(format!("{}", snapshot))?)
    });

    match snapshot {
        Ok(snapshot) => {
            unsafe { libc::strdup(snapshot.as_ptr()) }
        }
        Err(err) => raise_error_null!(error, err),
    }
}


#[derive(Clone)]
pub(crate) struct BackupSetup {
    pub host: String,
    pub store: String,
    pub user: Userid,
    pub chunk_size: u64,
    pub backup_type: String,
    pub backup_id: String,
    pub backup_time: i64,
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

    #[allow(clippy::mutex_atomic)]
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
            result,
        }
    }

    /// Waits until the callback from callback_info is called.
    pub fn wait(&mut self) {
        #[allow(clippy::mutex_atomic)]
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
        #[allow(clippy::mutex_atomic)]
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
    compress: bool,
    encrypt: bool,
    fingerprint: *const c_char,
    error: * mut * mut c_char,
) -> *mut ProxmoxBackupHandle {

    let task: Result<_, Error> = try_block!({
        let repo: BackupRepository = tools::utf8_c_string(repo)?
            .ok_or_else(|| format_err!("repo must not be NULL"))?
            .parse()?;

        let backup_id = tools::utf8_c_string(backup_id)?
            .ok_or_else(|| format_err!("backup_id must not be NULL"))?;

        let password = tools::utf8_c_string(password)?;
        let keyfile = tools::utf8_c_string(keyfile)?.map(std::path::PathBuf::from);
        let key_password = tools::utf8_c_string(key_password)?;
        let fingerprint = tools::utf8_c_string(fingerprint)?;

        let crypt_mode = if keyfile.is_some() {
            if encrypt { CryptMode::Encrypt } else {  CryptMode::SignOnly }
        } else {
            CryptMode::None
        };

        let setup = BackupSetup {
            host: repo.host().to_owned(),
            user: repo.user().to_owned(),
            store: repo.store().to_owned(),
            chunk_size: if chunk_size > 0 { chunk_size } else { PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE },
            backup_type: String::from("vm"),
            backup_id,
            password,
            backup_time: backup_time as i64,
            keyfile,
            key_password,
            fingerprint,
        };

        BackupTask::new(setup, compress, crypt_mode)
    });

    match task {
        Ok(task) => {
            let boxed_task = Box::new(Arc::new(task));
            Box::into_raw(boxed_task) as * mut ProxmoxBackupHandle
        }
        Err(err) => raise_error_null!(error, err),
    }
}

fn backup_handle_to_task(handle: *mut ProxmoxBackupHandle) -> Arc<BackupTask> {
    let task = unsafe { & *(handle as *const Arc<BackupTask>) };
    // increase reference count while we use it inside rust
    Arc::clone(task)
}

/// Open connection to the backup server (sync)
///
/// Returns:
///  0 ... Sucecss (no prevbious backup)
///  1 ... Success (found previous backup)
/// -1 ... Error
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_connect(
    handle: *mut ProxmoxBackupHandle,
    error: *mut *mut c_char,
) -> c_int {
    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    proxmox_backup_connect_async(
        handle,
        callback_info.callback,
        callback_info.callback_data,
        callback_info.result,
        callback_info.error,
    );

    got_result_condition.wait();

    result
}

/// Open connection to the backup server
///
/// Returns:
///  0 ... Sucecss (no prevbious backup)
///  1 ... Success (found previous backup)
/// -1 ... Error
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_connect_async(
    handle: *mut ProxmoxBackupHandle,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: *mut *mut c_char,
) {
    let task = backup_handle_to_task(handle);
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    task.runtime().spawn(async move {
        let result = task.connect().await;
        callback_info.send_result(result);
    });
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
    let task = backup_handle_to_task(handle);
    let reason = tools::utf8_c_string_lossy(reason)
        .unwrap_or_else(|| "no reason (NULL)".to_string());
    task.abort(reason);
}

/// Check if we can do incremental backups.
///
/// This method compares the csum from last backup manifest with the
/// checksum stored locally.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_check_incremental(
    handle: *mut ProxmoxBackupHandle,
    device_name: *const c_char, // expect utf8 here
    size: u64,
) -> c_int {
    let task = backup_handle_to_task(handle);

    if device_name.is_null() { return 0; }

    match tools::utf8_c_string_lossy(device_name) {
        None => 0,
        Some(device_name) => if task.check_incremental(device_name, size) { 1 } else { 0 },
    }
}

/// Register a backup image (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_register_image(
    handle: *mut ProxmoxBackupHandle,
    device_name: *const c_char, // expect utf8 here
    size: u64,
    incremental: bool,
    error: * mut * mut c_char,
) -> c_int {
    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    proxmox_backup_register_image_async(
        handle, device_name, size, incremental,
        callback_info.callback,
        callback_info.callback_data,
        callback_info.result,
        callback_info.error,
    );

    got_result_condition.wait();

    result
}

/// Register a backup image
///
/// Create a new image archive on the backup server
/// ('<device_name>.img.fidx'). The returned integer is the dev_id
/// parameter for the proxmox_backup_write_data_async() method.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_register_image_async(
    handle: *mut ProxmoxBackupHandle,
    device_name: *const c_char, // expect utf8 here
    size: u64,
    incremental: bool,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: * mut * mut c_char,
) {
    let task = backup_handle_to_task(handle);
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    param_not_null!(device_name, callback_info);

    let device_name = unsafe { tools::utf8_c_string_lossy_non_null(device_name) };

    task.runtime().spawn(async move {
        let result = task.register_image(device_name, size, incremental).await;
        callback_info.send_result(result);
    });
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
    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    proxmox_backup_add_config_async(
        handle, name, data, size,
        callback_info.callback,
        callback_info.callback_data,
        callback_info.result,
        callback_info.error,
    );

    got_result_condition.wait();

    result
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
    let task = backup_handle_to_task(handle);

    let callback_info = CallbackPointers { callback, callback_data, error, result };

    param_not_null!(name, callback_info);
    let name = unsafe { tools::utf8_c_string_lossy_non_null(name) };

    param_not_null!(data, callback_info);
    let data: Vec<u8> = unsafe { std::slice::from_raw_parts(data, size as usize).to_vec() };

    task.runtime().spawn(async move {
        let result = task.add_config(name, data).await;
        callback_info.send_result(result);
    });
}

/// Write data to into a registered image (sync)
///
/// Upload a chunk of data for the <dev_id> image.
///
/// The data pointer may be NULL in order to write the zero chunk
/// (only allowed if size == chunk_size)
///
/// Returns:
/// -1: on error
///  0: successful, chunk already exists on server, so it was resued
///  size: successful, chunk uploaded
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
    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    proxmox_backup_write_data_async(
        handle, dev_id, data, offset, size,
        callback_info.callback,
        callback_info.callback_data,
        callback_info.result,
        callback_info.error,
    );

    got_result_condition.wait();

    result
}

/// Write data to into a registered image
///
/// Upload a chunk of data for the <dev_id> image.
///
/// The data pointer may be NULL in order to write the zero chunk
/// (only allowed if size == chunk_size)
///
/// Note: The data pointer needs to be valid until the async
/// opteration is finished.
///
/// Returns:
/// -1: on error
///  0: successful, chunk already exists on server, so it was resued
///  size: successful, chunk uploaded
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
    let task = backup_handle_to_task(handle);
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    let data = DataPointer(data);

    task.runtime().spawn(async move {
        let result = task.write_data(dev_id, data, offset, size).await;
        callback_info.send_result(result);
    });
}

/// Close a registered image (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_close_image(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    error: * mut * mut c_char,
) -> c_int {
    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    proxmox_backup_close_image_async(
        handle, dev_id,
        callback_info.callback,
        callback_info.callback_data,
        callback_info.result,
        callback_info.error,
    );

    got_result_condition.wait();

    result
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
    let task = backup_handle_to_task(handle);
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    task.runtime().spawn(async move {
        let result = task.close_image(dev_id).await;
        callback_info.send_result(result);
    });
}

/// Finish the backup (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_finish(
    handle: *mut ProxmoxBackupHandle,
    error: * mut * mut c_char,
) -> c_int {
    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    proxmox_backup_finish_async(
        handle,
        callback_info.callback,
        callback_info.callback_data,
        callback_info.result,
        callback_info.error,
    );

    got_result_condition.wait();

    result
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
    let task = unsafe { & *(handle as * const Arc<BackupTask>) };
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    task.runtime().spawn(async move {
        let result = task.finish().await;
        callback_info.send_result(result);
    });
}

/// Disconnect and free allocated memory
///
/// The handle becomes invalid after this call.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_backup_disconnect(handle: *mut ProxmoxBackupHandle) {

    let task = handle as * mut Arc<BackupTask>;

    unsafe { Box::from_raw(task) }; // take ownership, drop(task)
}

// Simple interface to restore data
//
// currently only implemented for images...

fn restore_handle_to_task(handle: *mut ProxmoxRestoreHandle) -> Arc<RestoreTask> {
    let restore_task = unsafe { & *(handle as *const Arc<RestoreTask>) };
    // increase reference count while we use it inside rust
    Arc::clone(restore_task)
}

/// Connect the the backup server for restore (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_new(
    repo: *const c_char,
    snapshot: *const c_char,
    password: *const c_char,
    keyfile: *const c_char,
    key_password: *const c_char,
    fingerprint: *const c_char,
    error: * mut * mut c_char,
) -> *mut ProxmoxRestoreHandle {

    let result: Result<_, Error> = try_block!({
        let repo: BackupRepository = tools::utf8_c_string(repo)?
            .ok_or_else(|| format_err!("repo must not be NULL"))?
            .parse()?;

        let snapshot: BackupDir = tools::utf8_c_string_lossy(snapshot)
            .ok_or_else(|| format_err!("snapshot must not be NULL"))?
            .parse()?;

        let backup_type = snapshot.group().backup_type().to_owned();
        let backup_id = snapshot.group().backup_id().to_owned();
        let backup_time = snapshot.backup_time();

        let password = tools::utf8_c_string(password)?;
        let keyfile = tools::utf8_c_string(keyfile)?.map(std::path::PathBuf::from);
        let key_password = tools::utf8_c_string(key_password)?;
        let fingerprint = tools::utf8_c_string(fingerprint)?;

        let setup = BackupSetup {
            host: repo.host().to_owned(),
            user: repo.user().to_owned(),
            store: repo.store().to_owned(),
            chunk_size: PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE, // not used by restore
            backup_type,
            backup_id,
            password,
            backup_time,
            keyfile,
            key_password,
            fingerprint,
        };

        RestoreTask::new(setup)
    });

    match result {
        Ok(restore_task) => {
            let boxed_restore_task = Box::new(Arc::new(restore_task));
            Box::into_raw(boxed_restore_task) as * mut ProxmoxRestoreHandle
        }
        Err(err) => raise_error_null!(error, err),
    }
}

/// Open connection to the backup server (sync)
///
/// Returns:
///  0 ... Sucecss (no prevbious backup)
/// -1 ... Error
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_connect(
    handle: *mut ProxmoxRestoreHandle,
    error: *mut *mut c_char,
) -> c_int {
    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    proxmox_restore_connect_async(
        handle,
        callback_info.callback,
        callback_info.callback_data,
        callback_info.result,
        callback_info.error,
    );

    got_result_condition.wait();

    result
}
/// Open connection to the backup server (async)
///
/// Returns:
///  0 ... Sucecss (no prevbious backup)
/// -1 ... Error
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_connect_async(
    handle: *mut ProxmoxRestoreHandle,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: *mut *mut c_char,
) {
    let restore_task = restore_handle_to_task(handle);
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    restore_task.runtime().spawn(async move {
        let result = restore_task.connect().await;
        callback_info.send_result(result);
    });
}

/// Disconnect and free allocated memory
///
/// The handle becomes invalid after this call.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_disconnect(handle: *mut ProxmoxRestoreHandle) {

    let restore_task = handle as * mut Arc<RestoreTask>;
    unsafe { Box::from_raw(restore_task) }; //drop(restore_task)
}

/// Restore an image (sync)
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

    let restore_task = restore_handle_to_task(handle);

    let result: Result<_, Error> = try_block!({

        let archive_name = tools::utf8_c_string(archive_name)?
            .ok_or_else(|| format_err!("archive_name must not be NULL"))?;

        let write_data_callback = move |offset: u64, data: &[u8]| {
            callback(callback_data, offset, data.as_ptr(), data.len() as u64)
        };

        let write_zero_callback = move |offset: u64, len: u64| {
            callback(callback_data, offset, std::ptr::null(), len)
        };

        proxmox_backup::tools::runtime::block_on(
            restore_task.restore_image(archive_name, write_data_callback, write_zero_callback, verbose)
        )?;

        Ok(())
    });

    if let Err(err) = result {
        raise_error_int!(error, err);
    };

    0
}

/// Retrieve the ID of a handle used to access data in the given archive (sync)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_open_image(
    handle: *mut ProxmoxRestoreHandle,
    archive_name: *const c_char,
    error: * mut * mut c_char,
) -> c_int {
    let mut result: c_int = -1;
    let mut got_result_condition = GotResultCondition::new();
    let callback_info = got_result_condition.callback_info(&mut result, error);

    proxmox_restore_open_image_async(
        handle, archive_name,
        callback_info.callback,
        callback_info.callback_data,
        callback_info.result,
        callback_info.error,
    );

    got_result_condition.wait();

    result
}

/// Retrieve the ID of a handle used to access data in the given archive (async)
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_open_image_async(
    handle: *mut ProxmoxRestoreHandle,
    archive_name: *const c_char,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: * mut * mut c_char,
) {
    let restore_task = restore_handle_to_task(handle);
    let callback_info = CallbackPointers { callback, callback_data, error, result };

    param_not_null!(archive_name, callback_info);
    let archive_name = unsafe { tools::utf8_c_string_lossy_non_null(archive_name) };

    restore_task.runtime().spawn(async move {
        let result = match restore_task.open_image(archive_name).await {
            Ok(res) => Ok(res as i32),
            Err(err) => Err(err)
        };
        callback_info.send_result(result);
    });
}

/// Retrieve the length of a given archive handle in bytes
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_get_image_length(
    handle: *mut ProxmoxRestoreHandle,
    aid: u8,
    error: * mut * mut c_char,
) -> c_long {
    let restore_task = restore_handle_to_task(handle);
    let result = restore_task.get_image_length(aid);
    match result {
        Ok(res) => res as i64,
        Err(err) => raise_error_int!(error, err),
    }
}

/// Read data from the backup image at the given offset (sync)
///
/// Reads up to size bytes from handle aid at offset. On success,
/// returns the number of bytes read. (a return of zero indicates end
/// of file).
///
/// Note: It is not an error for a successful call to transfer fewer
/// bytes than requested.
#[no_mangle]
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_read_image_at(
    handle: *mut ProxmoxRestoreHandle,
    aid: u8,
    data: *mut u8,
    offset: u64,
    size: u64,
    error: * mut * mut c_char,
) -> c_int {
    let mut result: c_int = -1;

    let mut got_result_condition = GotResultCondition::new();

    let callback_info = got_result_condition.callback_info(&mut result, error);

    proxmox_restore_read_image_at_async(
        handle, aid, data, offset, size,
        callback_info.callback,
        callback_info.callback_data,
        callback_info.result,
        callback_info.error,
    );

    got_result_condition.wait();

    result
}

/// Read data from the backup image at the given offset (async)
///
/// Reads up to size bytes from handle aid at offset. On success,
/// returns the number of bytes read. (a return of zero indicates end
/// of file).
///
/// Note: The data pointer needs to be valid until the async
/// opteration is finished.
///
/// Note: The call will only ever transfer less than 'size' bytes if
/// the end of the file has been reached.
#[no_mangle]
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn proxmox_restore_read_image_at_async(
    handle: *mut ProxmoxRestoreHandle,
    aid: u8,
    data: *mut u8,
    offset: u64,
    size: u64,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    result: *mut c_int,
    error: * mut * mut c_char,
) {
    let restore_task = restore_handle_to_task(handle);

    let callback_info = CallbackPointers { callback, callback_data, error, result };

    param_not_null!(data, callback_info);
    let data = DataPointer(data);

    restore_task.runtime().spawn(async move {
        let result = restore_task.read_image_at(aid, data, offset, size).await;
        callback_info.send_result(result);
    });
}
