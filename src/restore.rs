use std::convert::TryInto;
use std::sync::{Arc, Mutex};

use anyhow::{bail, format_err, Error};
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;

use proxmox_async::runtime::get_runtime_with_builder;

use pbs_client::{BackupReader, HttpClient, HttpClientOptions, RemoteChunkReader};
use pbs_datastore::cached_chunk_reader::CachedChunkReader;
use pbs_datastore::data_blob::DataChunkBuilder;
use pbs_datastore::fixed_index::FixedIndexReader;
use pbs_datastore::index::IndexFile;
use pbs_datastore::read_chunk::ReadChunk;
use pbs_datastore::BackupManifest;
use pbs_key_config::load_and_decrypt_key;
use pbs_tools::crypt_config::CryptConfig;

use super::BackupSetup;
use crate::capi_types::DataPointer;
use crate::registry::Registry;
use crate::shared_cache::get_shared_chunk_cache;

struct ImageAccessInfo {
    reader: Arc<CachedChunkReader<FixedIndexReader, RemoteChunkReader>>,
    _archive_name: String,
    archive_size: u64,
}

pub(crate) struct RestoreTask {
    setup: BackupSetup,
    runtime: Arc<Runtime>,
    crypt_config: Option<Arc<CryptConfig>>,
    client: OnceCell<Arc<BackupReader>>,
    manifest: OnceCell<Arc<BackupManifest>>,
    image_registry: Arc<Mutex<Registry<ImageAccessInfo>>>,
}

impl RestoreTask {
    /// Create a new instance, using the specified Runtime
    ///
    /// We keep a reference to the runtime - else the runtime can be
    /// dropped and further connections fails.
    pub fn with_runtime(setup: BackupSetup, runtime: Arc<Runtime>) -> Result<Self, Error> {
        let crypt_config = match setup.keyfile {
            None => None,
            Some(ref path) => {
                let (key, _, _) = load_and_decrypt_key(path, &|| match setup.key_password {
                    Some(ref key_password) => Ok(key_password.as_bytes().to_vec()),
                    None => bail!("no key_password specified"),
                })?;
                Some(Arc::new(CryptConfig::new(key)?))
            }
        };

        Ok(Self {
            setup,
            runtime,
            crypt_config,
            client: OnceCell::new(),
            manifest: OnceCell::new(),
            image_registry: Arc::new(Mutex::new(Registry::<ImageAccessInfo>::new())),
        })
    }

    pub fn new(setup: BackupSetup) -> Result<Self, Error> {
        let runtime = get_runtime_with_builder(|| {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_all();
            builder.max_blocking_threads(2);
            builder.worker_threads(4);
            builder.thread_name("proxmox-restore-worker");
            builder
        });
        Self::with_runtime(setup, runtime)
    }

    pub async fn connect(&self) -> Result<libc::c_int, Error> {
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
        let client = BackupReader::start(
            &http,
            self.crypt_config.clone(),
            &self.setup.store,
            &self.setup.backup_ns,
            &self.setup.backup_dir,
            true,
        )
        .await?;

        let (manifest, _) = client.download_manifest().await?;
        manifest.check_fingerprint(self.crypt_config.as_ref().map(Arc::as_ref))?;

        self.manifest
            .set(Arc::new(manifest))
            .map_err(|_| format_err!("already connected!"))?;

        self.client
            .set(client)
            .map_err(|_| format_err!("already connected!"))?;

        Ok(0)
    }

    pub fn runtime(&self) -> tokio::runtime::Handle {
        self.runtime.handle().clone()
    }

    pub async fn restore_image(
        &self,
        archive_name: String,
        write_data_callback: impl Fn(u64, &[u8]) -> i32,
        write_zero_callback: impl Fn(u64, u64) -> i32,
        verbose: bool,
    ) -> Result<(), Error> {
        if verbose {
            eprintln!("download and verify backup index");
        }

        let client = match self.client.get() {
            Some(reader) => Arc::clone(reader),
            None => bail!("not connected"),
        };

        let manifest = match self.manifest.get() {
            Some(manifest) => Arc::clone(manifest),
            None => bail!("no manifest"),
        };

        let index = client
            .download_fixed_index(&manifest, &archive_name)
            .await?;

        let (_, zero_chunk_digest) = DataChunkBuilder::build_zero_chunk(
            self.crypt_config.as_ref().map(Arc::as_ref),
            index.chunk_size,
            true,
        )?;

        let most_used = index.find_most_used_chunks(8);

        let file_info = manifest.lookup_file_info(&archive_name)?;

        let chunk_reader = RemoteChunkReader::new(
            Arc::clone(&client),
            self.crypt_config.clone(),
            file_info.chunk_crypt_mode(),
            most_used,
        );

        let mut per = 0;
        let mut bytes = 0;
        let mut zeroes = 0;

        let start_time = std::time::Instant::now();

        for pos in 0..index.index_count() {
            let digest = index.index_digest(pos).unwrap();
            let offset = (pos * index.chunk_size) as u64;
            if digest == &zero_chunk_digest {
                let res = write_zero_callback(offset, index.chunk_size as u64);
                if res < 0 {
                    bail!("write_zero_callback failed ({})", res);
                }
                bytes += index.chunk_size;
                zeroes += index.chunk_size;
            } else {
                let raw_data = ReadChunk::read_chunk(&chunk_reader, digest)?;
                let res = write_data_callback(offset, &raw_data);
                if res < 0 {
                    bail!("write_data_callback failed ({})", res);
                }
                bytes += raw_data.len();
            }
            if verbose {
                let next_per = ((pos + 1) * 100) / index.index_count();
                if per != next_per {
                    eprintln!(
                        "progress {}% (read {} bytes, zeroes = {}% ({} bytes), duration {} sec)",
                        next_per,
                        bytes,
                        zeroes * 100 / bytes,
                        zeroes,
                        start_time.elapsed().as_secs()
                    );
                    per = next_per;
                }
            }
        }

        let end_time = std::time::Instant::now();
        let elapsed = end_time.duration_since(start_time);
        eprintln!(
            "restore image complete (bytes={}, duration={:.2}s, speed={:.2}MB/s)",
            bytes,
            elapsed.as_secs_f64(),
            bytes as f64 / (1024.0 * 1024.0 * elapsed.as_secs_f64())
        );

        Ok(())
    }

    pub fn get_image_length(&self, aid: u8) -> Result<u64, Error> {
        let mut guard = self.image_registry.lock().unwrap();
        let info = guard.lookup(aid)?;
        Ok(info.archive_size)
    }

    pub async fn open_image(&self, archive_name: String) -> Result<u8, Error> {
        let client = match self.client.get() {
            Some(reader) => Arc::clone(reader),
            None => bail!("not connected"),
        };

        let manifest = match self.manifest.get() {
            Some(manifest) => Arc::clone(manifest),
            None => bail!("no manifest"),
        };

        let index = client
            .download_fixed_index(&manifest, &archive_name)
            .await?;
        let archive_size = index.index_bytes();
        let most_used = index.find_most_used_chunks(8);

        let file_info = manifest.lookup_file_info(&archive_name)?;

        let chunk_reader = RemoteChunkReader::new(
            Arc::clone(&client),
            self.crypt_config.clone(),
            file_info.chunk_crypt_mode(),
            most_used,
        );

        let cache = get_shared_chunk_cache();
        let reader = Arc::new(CachedChunkReader::new_with_cache(
            chunk_reader,
            index,
            cache,
        ));

        let info = ImageAccessInfo {
            archive_size,
            _archive_name: archive_name,
            // useful to debug
            reader,
        };

        (*self.image_registry.lock().unwrap()).register(info)
    }

    pub async fn read_image_at(
        &self,
        aid: u8,
        data: DataPointer,
        offset: u64,
        size: u64,
    ) -> Result<libc::c_int, Error> {
        let (reader, image_size) = {
            let mut guard = self.image_registry.lock().unwrap();
            let info = guard.lookup(aid)?;
            (Arc::clone(&info.reader), info.archive_size)
        };

        if offset > image_size {
            bail!("read index {} out of bounds {}", offset, image_size);
        }

        let buf: &mut [u8] =
            unsafe { std::slice::from_raw_parts_mut(data.0 as *mut u8, size as usize) };
        let read = reader.read_at(buf, offset).await?;
        Ok(read.try_into()?)
    }
}
