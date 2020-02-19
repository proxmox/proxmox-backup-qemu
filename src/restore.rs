use failure::*;
use std::sync::Arc;
use std::os::unix::fs::OpenOptionsExt;

use proxmox_backup::tools::runtime::block_on;
use proxmox_backup::backup::*;
use proxmox_backup::client::{HttpClient, HttpClientOptions, BackupReader, RemoteChunkReader};

use super::BackupSetup;

pub(crate) struct ProxmoxRestore {
    pub client: Arc<BackupReader>,
    pub crypt_config: Option<Arc<CryptConfig>>,
    pub manifest: BackupManifest,
}

impl ProxmoxRestore {

    pub fn new(setup: BackupSetup) -> Result<Self, Error> {

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

        let host = setup.host;
        let user = setup.user;
        let store = setup.store;
        let backup_type = String::from("vm");
        let backup_id = setup.backup_id;
        let backup_time = setup.backup_time;

        let result: Result<_, Error> = block_on(async {

            let options = HttpClientOptions::new()
                .fingerprint(setup.fingerprint.clone())
                .password(setup.password.clone());

            let client = HttpClient::new(&host, &user, options)?;
            let client = BackupReader::start(
                client,
                crypt_config.clone(),
                &store,
                &backup_type,
                &backup_id,
                backup_time,
                true
            ).await?;

            let manifest = client.download_manifest().await?;

            Ok((client, manifest))
        });

        let (client, manifest) = result?;

        Ok(Self {
            manifest,
            client,
            crypt_config,
        })
    }

    pub fn restore(
        &mut self,
        archive_name: String,
        write_data_callback: impl Fn(u64, &[u8]) -> i32,
        write_zero_callback: impl Fn(u64, u64) -> i32,
        verbose: bool,
    ) -> Result<(), Error> {

        if !archive_name.ends_with(".img.fidx") {
            bail!("wrong archive type {:?}", archive_name);
        }

        block_on(
            self.restore_async(
                archive_name,
                write_data_callback,
                write_zero_callback,
                verbose,
            )
        )
    }

    async fn restore_async(
        &self,
        archive_name: String,
        write_data_callback: impl Fn(u64, &[u8]) -> i32,
        write_zero_callback: impl Fn(u64, u64) -> i32,
        verbose: bool,
    ) -> Result<(), Error> {

        let tmpfile = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .custom_flags(libc::O_TMPFILE)
            .open("/tmp")?;

        let tmpfile = self.client.download(&archive_name, tmpfile).await?;

        let index = FixedIndexReader::new(tmpfile)
            .map_err(|err| format_err!("unable to read fixed index '{}' - {}", archive_name, err))?;

        // Note: do not use values stored in index (not trusted) - instead, computed them again
        let (csum, size) = index.compute_csum();
        self.manifest.verify_file(&archive_name, &csum, size)?;

        let (_, zero_chunk_digest) = DataChunkBuilder::build_zero_chunk(
            self.crypt_config.as_ref().map(Arc::as_ref),
            index.chunk_size,
            true,
        )?;

        let most_used = index.find_most_used_chunks(8);

        let mut chunk_reader = RemoteChunkReader::new(
            self.client.clone(),
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
                let raw_data = chunk_reader.read_chunk(&digest)?;
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
}
