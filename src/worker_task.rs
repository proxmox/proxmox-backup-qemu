use anyhow::{bail, Error};
use std::collections::HashSet;
use std::sync::{Mutex, Arc};
use std::os::raw::c_int;

use futures::future::{Future, Either, FutureExt};

use proxmox_backup::backup::{CryptConfig, BackupManifest, load_and_decrypt_key};
use proxmox_backup::client::{HttpClient, HttpClientOptions, BackupWriter};

use super::BackupSetup;
use crate::capi_types::*;
use crate::commands::*;

pub(crate) struct BackupTask {
    setup: BackupSetup,
    runtime: tokio::runtime::Runtime,
    crypt_config: Option<Arc<CryptConfig>>,
    writer: Option<Arc<BackupWriter>>,
    last_manifest: Option<BackupManifest>,
    registry: ImageRegistry,
    known_chunks: Arc<Mutex<HashSet<[u8;32]>>>,
    abort: tokio::sync::broadcast::Sender<()>,
    aborted: Option<String>,  // set on abort, conatins abort reason
    written_bytes: u64,
}

impl BackupTask {

    pub fn new(setup: BackupSetup) -> Result<Self, Error> {

        let mut builder = tokio::runtime::Builder::new();
        builder.threaded_scheduler();
        builder.enable_all();
        builder.max_threads(6);
        builder.core_threads(4);
        builder.thread_name("proxmox-backup-qemu-worker");

        let runtime = builder.build()?;

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

        let (abort, _) = tokio::sync::broadcast::channel(16); // fixme: 16??

        let registry = ImageRegistry::new();
        let known_chunks = Arc::new(Mutex::new(HashSet::new()));

        Ok(Self { runtime, setup, crypt_config, abort, registry, known_chunks,
                  writer: None, written_bytes: 0,
                  last_manifest: None, aborted: None })
    }

    pub fn runtime(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }

    fn check_aborted(&self) -> Result<(), Error> {
        if self.aborted.is_some() {
            bail!("task already aborted");
        }
        Ok(())
    }

    pub fn abort(&mut self, reason: String) {
        let _ = self.abort.send(()); // fixme: ignore errors?
        if self.aborted.is_none() {
            self.aborted = Some(reason);
        }
    }

    async fn _connect(&mut self) -> Result<c_int, Error> {

        let options = HttpClientOptions::new()
            .fingerprint(self.setup.fingerprint.clone())
            .password(self.setup.password.clone());

        let http = HttpClient::new(&self.setup.host, &self.setup.user, options)?;
        let writer = BackupWriter::start(http, self.crypt_config.clone(), &self.setup.store, "vm", &self.setup.backup_id, self.setup.backup_time, false).await?;

        let last_manifest = writer.download_previous_manifest().await;
        if let Ok(last_manifest) = last_manifest {
            self.last_manifest = Some(last_manifest);
        }

        self.writer = Some(writer);

        Ok(if self.last_manifest.is_some() { 1 } else { 0 })
    }

    pub async fn connect(&mut self) -> Result<c_int, Error> {

        self.check_aborted()?;

        let mut abort_rx = self.abort.subscribe();
        abortable_command(Self::_connect(self), abort_rx.recv()).await
    }

    pub async fn add_config(
        &mut self,
        name: String,
        data: DataPointer, // fixme: use [u8]
        size: u64,
    ) -> Result<c_int, Error> {

        self.check_aborted()?;

        let writer = match self.writer {
            Some(ref writer) => writer.clone(),
            None => bail!("not connected"),
        };

        let command_future = add_config(
            writer,
            &mut self.registry,
            name,
            data,
            size);

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    pub async fn write_data(
        &mut self,
        dev_id: u8,
        data: DataPointer,
        offset: u64,
        size: u64,
    ) -> Result<c_int, Error> {

        self.check_aborted()?;

        let writer = match self.writer {
            Some(ref writer) => writer.clone(),
            None => bail!("not connected"),
        };

        self.written_bytes += size;

        let command_future = write_data(
            writer,
            self.crypt_config.clone(),
            &mut self.registry,
            self.known_chunks.clone(),
            dev_id,
            data,
            offset,
            size,
            self.setup.chunk_size,
        );

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    pub async fn register_image(
        &mut self,
        device_name: String,
        size: u64,
        incremental: bool,
    ) -> Result<c_int, Error> {

        self.check_aborted()?;

        let writer = match self.writer {
            Some(ref writer) => writer.clone(),
            None => bail!("not connected"),
        };

        let command_future = register_image(
            writer,
            self.crypt_config.clone(),
            &self.last_manifest,
            &mut self.registry,
            self.known_chunks.clone(),
            device_name,
            size,
            self.setup.chunk_size,
            incremental,
        );

        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    pub async fn close_image(
        &mut self,
        dev_id: u8,
    ) -> Result<c_int, Error> {

        self.check_aborted()?;

        let writer = match self.writer {
            Some(ref writer) => writer.clone(),
            None => bail!("not connected"),
        };

        let command_future = close_image(writer, &mut self.registry, dev_id);
        let mut abort_rx = self.abort.subscribe();
        abortable_command(command_future, abort_rx.recv()).await
    }

    pub async fn finish(
        &mut self,
    ) -> Result<c_int, Error> {

        self.check_aborted()?;

        let writer = match self.writer {
            Some(ref writer) => writer.clone(),
            None => bail!("not connected"),
        };

        let command_future = finish_backup(writer, &self.registry, &self.setup);
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
