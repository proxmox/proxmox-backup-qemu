use anyhow::{bail, format_err, Error};
use once_cell::sync::OnceCell;
use std::collections::HashSet;
use std::os::raw::c_int;
use std::sync::{Arc, Mutex};

use futures::future::{Either, Future, FutureExt};
use tokio::runtime::Runtime;

use proxmox_async::runtime::get_runtime_with_builder;
use proxmox_sys::fs::file_get_contents;

use pbs_api_types::{BackupType, CryptMode};
use pbs_client::{BackupWriter, HttpClient, HttpClientOptions};
use pbs_datastore::BackupManifest;
use pbs_key_config::{load_and_decrypt_key, rsa_encrypt_key_config, KeyConfig};
use pbs_tools::crypt_config::CryptConfig;

use super::BackupSetup;
use crate::capi_types::*;
use crate::commands::*;
use crate::registry::Registry;

pub(crate) struct BackupTask {
    setup: BackupSetup,
    runtime: Arc<Runtime>,
    compress: bool,
    crypt_mode: CryptMode,
    crypt_config: Option<Arc<CryptConfig>>,
    rsa_encrypted_key: Option<Vec<u8>>,
    writer: OnceCell<Arc<BackupWriter>>,
    last_manifest: OnceCell<Arc<BackupManifest>>,
    manifest: Arc<Mutex<BackupManifest>>,
    registry: Arc<Mutex<Registry<ImageUploadInfo>>>,
    known_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
    abort: tokio::sync::broadcast::Sender<()>,
    aborted: OnceCell<String>, // set on abort, contains abort reason
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
        runtime: Arc<Runtime>,
    ) -> Result<Self, Error> {
        let (crypt_config, rsa_encrypted_key) = match setup.keyfile {
            None => (None, None),
            Some(ref path) => {
                let (key, created, _) = load_and_decrypt_key(path, &|| match setup.key_password {
                    Some(ref key_password) => Ok(key_password.as_bytes().to_vec()),
                    None => bail!("no key_password specified"),
                })?;
                let rsa_encrypted_key = match setup.master_keyfile {
                    Some(ref master_keyfile) => {
                        let pem = file_get_contents(master_keyfile)?;
                        let rsa = openssl::rsa::Rsa::public_key_from_pem(&pem)?;

                        let mut key_config = KeyConfig::without_password(key)?;
                        key_config.created = created; // keep original value

                        Some(rsa_encrypt_key_config(rsa, &key_config)?)
                    }
                    None => None,
                };
                (Some(Arc::new(CryptConfig::new(key)?)), rsa_encrypted_key)
            }
        };

        let (abort, _) = tokio::sync::broadcast::channel(16);

        let manifest = Arc::new(Mutex::new(BackupManifest::new(setup.backup_dir.clone())));

        let registry = Arc::new(Mutex::new(Registry::<ImageUploadInfo>::new()));
        let known_chunks = Arc::new(Mutex::new(HashSet::new()));

        Ok(Self {
            runtime,
            setup,
            compress,
            crypt_mode,
            crypt_config,
            rsa_encrypted_key,
            abort,
            registry,
            manifest,
            known_chunks,
            writer: OnceCell::new(),
            last_manifest: OnceCell::new(),
            aborted: OnceCell::new(),
        })
    }

    pub fn new(setup: BackupSetup, compress: bool, crypt_mode: CryptMode) -> Result<Self, Error> {
        let runtime = get_runtime_with_builder(|| {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_all();
            builder.max_blocking_threads(2);
            builder.worker_threads(4);
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
        if self.abort.send(()).is_err() {
            // should not happen, but log to stderr
            eprintln!("BackupTask send abort failed.");
        }
        let _ = self.aborted.set(reason);
    }

    pub async fn connect(&self) -> Result<c_int, Error> {
        self.check_aborted()?;

        let command_future = async {
            let options = HttpClientOptions::new_non_interactive(
                self.setup.password.clone(),
                self.setup.fingerprint.clone(),
            );

            let http = HttpClient::new(
                &self.setup.host,
                self.setup.port,
                &self.setup.auth_id,
                options,
            )?;
            let mut backup_dir = self.setup.backup_dir.clone();
            backup_dir.group.ty = BackupType::Vm;
            let writer = BackupWriter::start(
                &http,
                self.crypt_config.clone(),
                &self.setup.store,
                &self.setup.backup_ns,
                &backup_dir,
                false,
                false,
            )
            .await?;

            let last_manifest = writer.download_previous_manifest().await;
            let mut result = 0;
            if let Ok(last_manifest) = last_manifest {
                result = 1;
                self.last_manifest
                    .set(Arc::new(last_manifest))
                    .map_err(|_| format_err!("already connected!"))?;
            }

            self.writer
                .set(writer)
                .map_err(|_| format_err!("already connected!"))?;

            Ok(result)
        };

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    fn need_writer(&self) -> Result<Arc<BackupWriter>, Error> {
        self.writer
            .get()
            .map(Arc::clone)
            .ok_or_else(|| format_err!("not connected"))
    }

    pub async fn add_config(&self, name: String, data: Vec<u8>) -> Result<c_int, Error> {
        self.check_aborted()?;

        let command_future = add_config(
            self.need_writer()?,
            Arc::clone(&self.manifest),
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

        let command_future = write_data(
            self.need_writer()?,
            if self.crypt_mode == CryptMode::Encrypt {
                self.crypt_config.clone()
            } else {
                None
            },
            Arc::clone(&self.registry),
            Arc::clone(&self.known_chunks),
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
        self.last_manifest.get().map(Arc::clone)
    }

    pub fn check_incremental(&self, device_name: String, size: u64) -> bool {
        match self.last_manifest() {
            Some(ref manifest) => {
                let archive_name = if let Ok(archive) = archive_name_from_device_name(&device_name)
                {
                    archive
                } else {
                    return false;
                };

                check_last_incremental_csum(Arc::clone(manifest), &archive_name, &device_name, size)
                    && check_last_encryption_mode(
                        Arc::clone(manifest),
                        &archive_name,
                        self.crypt_mode,
                    )
                    && check_last_encryption_key(self.crypt_config.clone())
            }
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

        let command_future = register_image(
            self.need_writer()?,
            self.crypt_config.clone(),
            self.crypt_mode,
            self.last_manifest.get().map(Arc::clone),
            Arc::clone(&self.registry),
            Arc::clone(&self.known_chunks),
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

        let command_future = close_image(
            self.need_writer()?,
            Arc::clone(&self.manifest),
            Arc::clone(&self.registry),
            dev_id,
            self.crypt_mode,
        );

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    pub async fn finish(&self) -> Result<c_int, Error> {
        self.check_aborted()?;

        let command_future = finish_backup(
            self.need_writer()?,
            self.crypt_config.clone(),
            self.rsa_encrypted_key.clone(),
            Arc::clone(&self.manifest),
        );

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }
}

fn abortable_command<'a, F: 'a + Send + Future<Output = Result<c_int, Error>>>(
    command_future: F,
    abort_future: impl 'a + Send + Future<Output = Result<(), tokio::sync::broadcast::error::RecvError>>,
) -> impl 'a + Future<Output = Result<c_int, Error>> {
    futures::future::select(command_future.boxed(), abort_future.boxed()).map(move |either| {
        match either {
            Either::Left((result, _)) => result,
            Either::Right(_) => bail!("command aborted"),
        }
    })
}
