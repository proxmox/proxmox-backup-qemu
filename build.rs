// build.rs
extern crate cbindgen;

use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let header = std::fs::read_to_string("header-preamble.c").unwrap();

    let crate_ver = env!("CARGO_PKG_VERSION");
    let git_ver = match option_env!("GITVERSION") {
        Some(ver) if !ver.is_empty() => ver,
        _ => "UNKNOWN",
    };
    let version_string = format!(
        "{} ({})",
        crate_ver,
        git_ver,
    );

    cbindgen::Builder::new()
        .with_language(cbindgen::Language::C)
        .with_crate(&crate_dir)
        .with_header(header)
        .with_include_guard("PROXMOX_BACKUP_QEMU_H")
        .generate()
        .unwrap()
        .write_to_file("proxmox-backup-qemu.h");

    println!("cargo:rustc-cdylib-link-arg=-Wl,-soname,libproxmox_backup_qemu.so.0");
    println!("cargo:rustc-env=PBS_LIB_VERSION={}", version_string);
}
