use std::os::raw::{c_char, c_void};

pub(crate) struct CallbackPointers {
    pub callback: extern "C" fn(*mut c_void),
    pub callback_data: *mut c_void,
    pub error: * mut *mut c_char,
}
unsafe impl std::marker::Send for CallbackPointers {}

pub(crate) struct DataPointer (pub *const u8);
unsafe impl std::marker::Send for DataPointer {}

#[repr(C)]
pub struct ProxmoxBackupHandle;
