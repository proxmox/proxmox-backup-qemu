use failure::*;
use std::os::raw::{c_char, c_void};
use std::sync::{Mutex, Arc, mpsc::Sender };
use std::ffi::CString;

pub(crate) struct CallbackPointers {
    pub callback: extern "C" fn(*mut c_void),
    pub callback_data: *mut c_void,
    pub error: * mut *mut c_char,
}
unsafe impl std::marker::Send for CallbackPointers {}

impl CallbackPointers {

    pub fn send_result(self, result: Result<(), Error>) {
        match result {
            Ok(_) => {
                unsafe { *(self.error) = std::ptr::null_mut(); }
                (self.callback)(self.callback_data);
            }
            Err(err) => {
                let errmsg = CString::new(format!("command error: {}", err)).unwrap();
                unsafe { *(self.error) = errmsg.into_raw(); }
                (self.callback)(self.callback_data);
            }
        }
    }
}

pub(crate) struct DataPointer (pub *const u8);
unsafe impl std::marker::Send for DataPointer {}

/// Opaque handle for restore jobs
#[repr(C)]
pub struct ProxmoxRestoreHandle;

/// Opaque handle for backups jobs
#[repr(C)]
pub struct ProxmoxBackupHandle;

pub(crate) enum BackupMessage {
    End,
    Abort,
    Connect {
        callback_info: CallbackPointers,
    },
    AddConfig {
        name: String,
        data: DataPointer,
        size: u64,
        result_channel: Arc<Mutex<Sender<Result<(), Error>>>>,
    },
    RegisterImage {
        device_name: String,
        size: u64,
        result_channel: Arc<Mutex<Sender<Result<u8, Error>>>>,
    },
    CloseImage {
        dev_id: u8,
        callback_info: CallbackPointers,
    },
    WriteData {
        dev_id: u8,
        data: DataPointer,
        offset: u64,
        size: u64,
        callback_info: CallbackPointers,
    },
    Finish {
        callback_info: CallbackPointers,
    },
}
