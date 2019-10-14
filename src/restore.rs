use failure::*;
use std::sync::Arc;
use std::os::unix::fs::OpenOptionsExt;

use proxmox_backup::backup::*;
use proxmox_backup::client::{HttpClient, BackupReader, BackupRepository, RemoteChunkReader};

fn get_encryption_key_password() -> Result<Vec<u8>, Error> {
    use std::env::VarError::*;
    match std::env::var("PBS_ENCRYPTION_PASSWORD") {
        Ok(p) => return Ok(p.as_bytes().to_vec()),
        Err(NotUnicode(_)) => bail!("PBS_ENCRYPTION_PASSWORD contains bad characters"),
        Err(NotPresent) => {
            bail!("env PBS_ENCRYPTION_PASSWORD not set");
        }
    }
}

pub async fn restore_async(
    repo: BackupRepository,
    snapshot: BackupDir,
    archive_name: String,
    crypt_config: Option<Arc<CryptConfig>>,
    write_data_callback: impl Fn(u64, &[u8]) -> i32,
    write_zero_callback: impl Fn(u64, u64) -> i32,
    verbose: bool,
) -> Result<(), Error> {

    if !archive_name.ends_with(".img.fidx") {
        bail!("wrong archive type {:?}", archive_name);
    }

    let client = HttpClient::new(repo.host(), repo.user(), None)?;
    let client = BackupReader::start(
        client,
        crypt_config.clone(),
        repo.store(),
        snapshot.group().backup_type(),
        snapshot.group().backup_id(),
        snapshot.backup_time(),
        true
    ).await?;

    let manifest = client.download_manifest().await?;

    let tmpfile = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .custom_flags(libc::O_TMPFILE)
        .open("/tmp")?;

    let tmpfile = client.download(&archive_name, tmpfile).await?;

    let index = FixedIndexReader::new(tmpfile)
        .map_err(|err| format_err!("unable to read fixed index '{}' - {}", archive_name, err))?;

    // Note: do not use values stored in index (not trusted) - instead, computed them again
    let (csum, size) = index.compute_csum();
    manifest.verify_file(&archive_name, &csum, size)?;

    let (_, zero_chunk_digest) = DataChunkBuilder::build_zero_chunk(
        crypt_config.as_ref().map(Arc::as_ref),
        index.chunk_size,
        true,
    )?;

    let most_used = index.find_most_used_chunks(8);

    let mut chunk_reader = RemoteChunkReader::new(client.clone(), crypt_config, most_used);

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

pub fn restore(
    repo: BackupRepository,
    snapshot: BackupDir,
    archive_name: String,
    keyfile: Option<std::path::PathBuf>,
    write_data_callback: impl Fn(u64, &[u8]) -> i32,
    write_zero_callback: impl Fn(u64, u64) -> i32,
    verbose: bool,
) -> Result<(), Error> {

    let crypt_config = match keyfile {
        None => None,
        Some(path) => {
            let (key, _) = load_and_decrtypt_key(&path, get_encryption_key_password)?;
            Some(Arc::new(CryptConfig::new(key)?))
        }
    };

    let mut builder = tokio::runtime::Builder::new();
    builder.core_threads(4);
    builder.name_prefix("pbs-restore-");

    let runtime = builder
        .build()
        .map_err(|err| format_err!("create runtime failed - {}", err))?;

    let result = runtime.block_on(
        restore_async(
            repo,
            snapshot,
            archive_name,
            crypt_config,
            write_data_callback,
            write_zero_callback,
            verbose,
        )
    );

    result
}
