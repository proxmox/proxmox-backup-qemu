use failure::*;
use std::os::raw::{c_char, c_void, c_int};
use std::ffi::CString;

pub(crate) struct CallbackPointers {
    pub callback: extern "C" fn(*mut c_void),
    pub callback_data: *mut c_void,
    pub result: *mut c_int,
    pub error: *mut *mut c_char,
}
unsafe impl std::marker::Send for CallbackPointers {}

impl CallbackPointers {

    pub fn send_result(self, result: Result<c_int, Error>) {
        match result {
            Ok(ret) => {
                unsafe {
                    if !self.result.is_null() {
                        *(self.result) = ret;
                    }
                }
                (self.callback)(self.callback_data);
            }
            Err(err) => {
                let errmsg = CString::new(format!("command error: {}", err)).unwrap();
                unsafe {
                    if !self.result.is_null() {
                        *(self.result) = -1;
                    }
                    if !self.error.is_null() {
                        *(self.error) = errmsg.into_raw();
                    }
                }
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
        callback_info: CallbackPointers,
    },
    RegisterImage {
        device_name: String,
        size: u64,
        callback_info: CallbackPointers,
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
