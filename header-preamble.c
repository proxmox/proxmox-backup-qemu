/*
 * A Proxmox Backup Server C interface, intended for use inside Qemu
 *
 * Copyright (C) 2019 Proxmox Server Solutions GmbH
 *
 * Authors:
 *  Dietmar Maurer (dietmar@proxmox.com)
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 *
 *
 * NOTE: Async Commands
 *
 * Some command are asynchronous (marked as _async). They run in a
 * separate thread and have the following parameters:
 *
 * callback: extern "C" fn(*mut c_void),
 * callback_data: *mut c_void,
 * error: * mut * mut c_char,
 *
 * The callback function is called when the the async function is
 * ready. Possible errors resturned in 'error'.
 */
