use anyhow::{format_err, bail, Error};
use std::collections::HashSet;
use std::sync::{Mutex, Arc};
use once_cell::sync::OnceCell;
use std::os::raw::c_int;

use futures::future::{Future, Either, FutureExt};
use tokio::runtime::Runtime;

use proxmox_backup::tools::runtime::get_runtime_with_builder;
use proxmox_backup::backup::{CryptConfig, CryptMode, BackupDir, BackupManifest, load_and_decrypt_key};
use proxmox_backup::client::{HttpClient, HttpClientOptions, BackupWriter};

use super::BackupSetup;
use crate::capi_types::*;
use crate::registry::Registry;
use crate::commands::*;

pub(crate) struct BackupTask {
    setup: BackupSetup,
    runtime: Arc<Runtime>,
    compress: bool,
    crypt_mode: CryptMode,
    crypt_config: Option<Arc<CryptConfig>>,
    writer: OnceCell<Arc<BackupWriter>>,
    last_manifest: OnceCell<Arc<BackupManifest>>,
    manifest: Arc<Mutex<BackupManifest>>,
    registry: Arc<Mutex<Registry<ImageUploadInfo>>>,
    known_chunks: Arc<Mutex<HashSet<[u8;32]>>>,
    abort: tokio::sync::broadcast::Sender<()>,
    aborted: OnceCell<String>,  // set on abort, conatins abort reason
}

impl BackupTask {

    /// Create a new instance, using the specified Runtime
    ///
    /// We keep a reference to the runtime - else the runtime can be
    /// dropped and further connections fails.
    pub fn with_runtime(
        setup: BackupSetup,
        compress: bool,
        crypt_mode: CryptMode,
        runtime: Arc<Runtime>
    ) -> Result<Self, Error> {

        let crypt_config = match setup.keyfile {
            None => None,
            Some(ref path) => {
                let (key, _) = load_and_decrypt_key(path, & || {
                    match setup.key_password {
                        Some(ref key_password) => Ok(key_password.as_bytes().to_vec()),
                        None => bail!("no key_password specified"),
                    }
                })?;
                Some(Arc::new(CryptConfig::new(key)?))
            }
        };

        let (abort, _) = tokio::sync::broadcast::channel(16);

        let snapshot = BackupDir::new(&setup.backup_type, &setup.backup_id, setup.backup_time.timestamp());
        let manifest = Arc::new(Mutex::new(BackupManifest::new(snapshot)));

        let registry = Arc::new(Mutex::new(Registry::<ImageUploadInfo>::new()));
        let known_chunks = Arc::new(Mutex::new(HashSet::new()));

        Ok(Self { runtime, setup, compress, crypt_mode, crypt_config, abort,
                  registry, manifest, known_chunks,
                  writer: OnceCell::new(), last_manifest: OnceCell::new(),
                  aborted: OnceCell::new() })
    }

    pub fn new(setup: BackupSetup, compress: bool, crypt_mode: CryptMode) -> Result<Self, Error> {
        let runtime = get_runtime_with_builder(|| {
            let mut builder = tokio::runtime::Builder::new();
            builder.threaded_scheduler();
            builder.enable_all();
            builder.max_threads(6);
            builder.core_threads(4);
            builder.thread_name("proxmox-backup-worker");
            builder
        });
        Self::with_runtime(setup, compress, crypt_mode, runtime)
    }

    pub fn runtime(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }

    fn check_aborted(&self) -> Result<(), Error> {
        if self.aborted.get().is_some() {
            bail!("task already aborted");
        }
        Ok(())
    }

    pub fn abort(&self, reason: String) {
        if let Err(_) = self.abort.send(()) {
            // should not happen, but log to stderr
            eprintln!("BackupTask send abort failed.");
        }
        let _ = self.aborted.set(reason);
    }

    pub async fn connect(&self) -> Result<c_int, Error> {

        self.check_aborted()?;

        let command_future = async  {
            let options = HttpClientOptions::new()
                .fingerprint(self.setup.fingerprint.clone())
                .password(self.setup.password.clone());

            let http = HttpClient::new(&self.setup.host, &self.setup.user, options)?;
            let writer = BackupWriter::start(
                http, self.crypt_config.clone(), &self.setup.store, "vm", &self.setup.backup_id,
                self.setup.backup_time, false).await?;

            let last_manifest = writer.download_previous_manifest().await;
            let mut result = 0;
            if let Ok(last_manifest) = last_manifest {
                result = 1;
                self.last_manifest.set(Arc::new(last_manifest))
                    .map_err(|_| format_err!("already connected!"))?;
            }

            self.writer.set(writer)
                .map_err(|_| format_err!("already connected!"))?;

            Ok(result)
        };

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    pub async fn add_config(
        &self,
        name: String,
        data: Vec<u8>,
    ) -> Result<c_int, Error> {

        self.check_aborted()?;

        let writer = match self.writer.get() {
            Some(writer) => writer.clone(),
            None => bail!("not connected"),
        };

        let command_future = add_config(
            writer,
            self.manifest.clone(),
            name,
            data,
            self.compress,
            self.crypt_mode,
        );

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    pub async fn write_data(
        &self,
        dev_id: u8,
        data: DataPointer, // this may be null
        offset: u64,
        size: u64,
    ) -> Result<c_int, Error> {

        self.check_aborted()?;

        let writer = match self.writer.get() {
            Some(writer) => writer.clone(),
            None => bail!("not connected"),
        };

        let command_future = write_data(
            writer,
            if self.crypt_mode == CryptMode::Encrypt { self.crypt_config.clone() } else { None },
            self.registry.clone(),
            self.known_chunks.clone(),
            dev_id,
            data,
            offset,
            size,
            self.setup.chunk_size,
            self.compress,
        );

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    fn last_manifest(&self) -> Option<Arc<BackupManifest>> {
        self.last_manifest.get().map(|m| m.clone())
    }

    pub fn check_incremental(
        &self,
        device_name: String,
        size: u64,
    ) -> bool {
        match self.last_manifest() {
            Some(ref manifest) => {
                check_last_incremental_csum(manifest.clone(), &device_name, size)
                    && check_last_encryption_mode(manifest.clone(), &device_name, self.crypt_mode)
            },
            None => false,
        }
    }

    pub async fn register_image(
        &self,
        device_name: String,
        size: u64,
        incremental: bool,
    ) -> Result<c_int, Error> {

        self.check_aborted()?;

        let writer = match self.writer.get() {
            Some(writer) => writer.clone(),
            None => bail!("not connected"),
        };

        let command_future = register_image(
            writer,
            self.crypt_config.clone(),
            self.crypt_mode,
            self.last_manifest.get().map(|m| m.clone()),
            self.registry.clone(),
            self.known_chunks.clone(),
            device_name,
            size,
            self.setup.chunk_size,
            incremental,
        );

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    pub async fn close_image(&self, dev_id: u8) -> Result<c_int, Error> {

        self.check_aborted()?;

        let writer = match self.writer.get() {
            Some(writer) => writer.clone(),
            None => bail!("not connected"),
        };

        let command_future = close_image(
            writer,
            self.manifest.clone(),
            self.registry.clone(),
            dev_id,
            self.crypt_mode,
        );

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    pub async fn finish(&self) -> Result<c_int, Error> {

        self.check_aborted()?;

        let writer = match self.writer.get() {
            Some(writer) => writer.clone(),
            None => bail!("not connected"),
        };

        let command_future = finish_backup(writer, self.crypt_config.clone(), self.manifest.clone());

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

}

fn abortable_command<'a, F: 'a + Send + Future<Output=Result<c_int, Error>>>(
    command_future: F,
    abort_future: impl 'a + Send + Future<Output=Result<(), tokio::sync::broadcast::RecvError>>,
) -> impl 'a + Future<Output = Result<c_int, Error>> {

    futures::future::select(command_future.boxed(), abort_future.boxed())
        .map(move |either| {
            match either {
                Either::Left((result, _)) => {
                    result
                }
                Either::Right(_) => bail!("command aborted"),
            }
        })
}
