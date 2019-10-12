use failure::*;
use std::sync::Arc;

use proxmox_backup::backup::*;
use proxmox_backup::client::{HttpClient, BackupReader, BackupRepository};

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
) -> Result<(), Error> {

    let client = HttpClient::new(repo.host(), repo.user(), None)?;
    let client = BackupReader::start(
        client,
        repo.store(),
        snapshot.group().backup_type(),
        snapshot.group().backup_id(),
        snapshot.backup_time(),
        true
    ).await?;

    bail!("implement me");
    
    Ok(())
}

pub fn restore(
    repo: BackupRepository,
    snapshot: BackupDir,
    archive_name: String,
    keyfile: Option<std::path::PathBuf>,
    write_data_callback: impl Fn(u64, &[u8]) -> i32,
    write_zero_callback: impl Fn(u64, u64) -> i32,
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
        )
    );

    result
}
