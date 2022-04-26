use std::ffi::CStr;

use anyhow::Error;
use libc::c_char;

pub fn utf8_c_string(ptr: *const c_char) -> Result<Option<String>, Error> {
    Ok(if ptr.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(ptr).to_str()?.to_owned() })
    })
}

pub unsafe fn utf8_c_string_lossy_non_null(ptr: *const c_char) -> String {
    CStr::from_ptr(ptr).to_string_lossy().into_owned()
}

pub fn utf8_c_string_lossy(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { utf8_c_string_lossy_non_null(ptr) })
    }
}
