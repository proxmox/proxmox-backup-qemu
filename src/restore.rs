use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::convert::TryInto;

use anyhow::{format_err, bail, Error};
use once_cell::sync::OnceCell;

use proxmox_backup::tools::runtime::{get_runtime, block_in_place};
use proxmox_backup::backup::*;
use proxmox_backup::client::{HttpClient, HttpClientOptions, BackupReader, RemoteChunkReader};

use super::BackupSetup;
use crate::commands::Registry;
use crate::capi_types::DataPointer;

pub struct ImageAccessInfo {
    pub reader: Arc<Mutex<BufferedFixedReader<RemoteChunkReader>>>,
    pub archive_name: String,
    pub archive_size: u64,
}

pub(crate) struct ProxmoxRestore {
    setup: BackupSetup,
    runtime: Arc<tokio::runtime::Runtime>,
    pub crypt_config: Option<Arc<CryptConfig>>,
    pub client: OnceCell<Arc<BackupReader>>,
    pub manifest: OnceCell<Arc<BackupManifest>>,
    pub image_registry: Arc<Mutex<Registry<ImageAccessInfo>>>,
}

impl ProxmoxRestore {

    pub fn new(setup: BackupSetup) -> Result<Self, Error> {

        // keep a reference to the runtime - else the runtime can be dropped
        // and further connections fails.
        let runtime = get_runtime();

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

        Ok(Self {
            setup,
            runtime,
            crypt_config,
            client: OnceCell::new(),
            manifest: OnceCell::new(),
            image_registry: Arc::new(Mutex::new(Registry::<ImageAccessInfo>::new())),
        })
    }

    pub async fn connect(&self) -> Result<libc::c_int, Error> {

        let options = HttpClientOptions::new()
            .fingerprint(self.setup.fingerprint.clone())
            .password(self.setup.password.clone());

        let http = HttpClient::new(&self.setup.host, &self.setup.user, options)?;
        let client = BackupReader::start(
            http,
            self.crypt_config.clone(),
            &self.setup.store,
            "vm", // fixme:
            &self.setup.backup_id,
            self.setup.backup_time,
            true
        ).await?;

        let manifest = client.download_manifest().await?;

        self.manifest.set(Arc::new(manifest))
            .map_err(|_| format_err!("already connected!"))?;

        self.client.set(client)
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
            Some(reader) => reader.clone(),
            None => bail!("not connected"),
        };

        let manifest = match self.manifest.get() {
            Some(manifest) => manifest.clone(),
            None => bail!("no manifest"),
        };

        let index = client.download_fixed_index(&manifest, &archive_name).await?;

        let (_, zero_chunk_digest) = DataChunkBuilder::build_zero_chunk(
            self.crypt_config.as_ref().map(Arc::as_ref),
            index.chunk_size,
            true,
        )?;

        let most_used = index.find_most_used_chunks(8);

        let mut chunk_reader = RemoteChunkReader::new(
            client.clone(),
            self.crypt_config.clone(),
            most_used,
        );

        let mut per = 0;
        let mut bytes = 0;
        let mut zeroes = 0;

        let start_time = std::time::Instant::now();

        for pos in 0..index.index_count() {
            let digest = index.index_digest(pos).unwrap();
            let offset = (pos*index.chunk_size) as u64;
            if digest == &zero_chunk_digest {
                let res = write_zero_callback(offset, index.chunk_size as u64);
                if res < 0 {
                    bail!("write_zero_callback failed ({})", res);
                }
                bytes += index.chunk_size;
                zeroes += index.chunk_size;
            } else {
                let raw_data = ReadChunk::read_chunk(&mut chunk_reader, &digest)?;
                let res = write_data_callback(offset, &raw_data);
                if res < 0 {
                    bail!("write_data_callback failed ({})", res);
                }
                bytes += raw_data.len();
            }
            if verbose {
                let next_per = ((pos+1)*100)/index.index_count();
                if per != next_per {
                    eprintln!("progress {}% (read {} bytes, zeroes = {}% ({} bytes), duration {} sec)",
                              next_per, bytes,
                              zeroes*100/bytes, zeroes,
                              start_time.elapsed().as_secs());
                    per = next_per;
                }
            }
        }

        let end_time = std::time::Instant::now();
        let elapsed = end_time.duration_since(start_time);
        eprintln!("restore image complete (bytes={}, duration={:.2}s, speed={:.2}MB/s)",
                  bytes,
                  elapsed.as_secs_f64(),
                  bytes as f64/(1024.0*1024.0*elapsed.as_secs_f64())
        );

        Ok(())
    }

    pub fn get_image_length(&self, aid: u8) -> Result<u64, Error> {
        let mut guard = self.image_registry.lock().unwrap();
        let info = guard.lookup(aid)?;
        Ok(info.archive_size)
    }

    pub async fn open_image(
        &self,
        archive_name: String,
    ) -> Result<u8, Error> {

        let client = match self.client.get() {
            Some(reader) => reader.clone(),
            None => bail!("not connected"),
        };

        let manifest = match self.manifest.get() {
            Some(manifest) => manifest.clone(),
            None => bail!("no manifest"),
        };

        let index = client.download_fixed_index(&manifest, &archive_name).await?;
        let archive_size = index.index_bytes();

        // fixme: use one chunk reader for all images
        let chunk_reader = RemoteChunkReader::new(
            client.clone(),
            self.crypt_config.clone(),
            HashMap::with_capacity(0),
        );

        let reader = BufferedFixedReader::new(index, chunk_reader);

        let info = ImageAccessInfo {
            archive_name, archive_size,
            reader: Arc::new(Mutex::new(reader)),
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
            (info.reader.clone(), info.archive_size)
        };

        if offset > image_size {
            bail!("read index {} out of bounds {}", offset, image_size);
        }

        let bytes = block_in_place(|| {
            let mut reader = reader.lock().unwrap();
            reader.seek(SeekFrom::Start(offset))?;
            let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(data.0 as *mut u8, size as usize)};
            reader.read(buf)
        })?;

        Ok(bytes.try_into()?)
    }
}
