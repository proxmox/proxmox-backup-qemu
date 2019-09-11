// build.rs
extern crate cbindgen;

use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_language(cbindgen::Language::C)
        .with_crate(&crate_dir)
        .generate()
        .unwrap()
        .write_to_file("proxmox-backup-qemu.h");
}
