// build.rs
extern crate cbindgen;

use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let header = std::fs::read_to_string("header-preamble.c").unwrap();

    cbindgen::Builder::new()
        .with_language(cbindgen::Language::C)
        .with_crate(&crate_dir)
        .with_header(header)
        .generate()
        .unwrap()
        .write_to_file("proxmox-backup-qemu.h");

    println!("cargo:rustc-cdylib-link-arg=-Wl,-soname,libproxmox_backup_qemu.so.0");
}
