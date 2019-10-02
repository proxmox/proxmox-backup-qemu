use failure::*;
use std::collections::HashSet;
use std::sync::{Mutex, Arc};
use std::ptr;
use std::ffi::CString;

use futures::future::{Future, TryFutureExt};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use proxmox_backup::backup::*;
use proxmox_backup::client::*;

use chrono::{Utc, DateTime};

use crate::capi_types::*;
use crate::upload_queue::*;

#[derive(Clone)]
pub(crate) struct BackupSetup {
    pub host: String,
    pub store: String,
    pub user: String,
    pub chunk_size: u64,
    pub backup_id: String,
    pub backup_time: DateTime<Utc>,
    pub password: CString,
    pub crypt_config: Option<Arc<CryptConfig>>,
}

struct ImageUploadInfo {
    wid: u64,
    device_name: String,
    zero_chunk_digest: [u8; 32],
    device_size: u64,
    upload_queue: Option<mpsc::Sender<Box<dyn Future<Output = Result<ChunkUploadInfo, Error>> + Send + Unpin>>>,
    upload_result: Option<oneshot::Receiver<Result<UploadResult, Error>>>,
}

pub (crate) struct ImageRegistry {
    upload_info: Vec<ImageUploadInfo>,
    file_list: Vec<Value>,
}

impl ImageRegistry {

    pub fn new() -> Self {
        Self {
            upload_info: Vec::new(),
            file_list: Vec::new(),
        }
    }

    fn register(&mut self, info: ImageUploadInfo) -> Result<u8, Error> {
        let dev_id = self.upload_info.len();
        if dev_id > 255 {
            bail!("register image faild - to many images (limit is 255)");
        }
        self.upload_info.push(info);
        Ok(dev_id as u8)
    }

    fn lookup(&mut self, dev_id: u8) -> Result<&mut ImageUploadInfo, Error> {
        if dev_id as usize >= self.upload_info.len() {
            bail!("image lookup failed for dev_id = {}", dev_id);
        }
        Ok(&mut self.upload_info[dev_id as usize])
    }
}

// Note: We alway register/upload a chunk containing zeros
async fn register_zero_chunk(
    client: Arc<BackupClient>,
    crypt_config: Option<Arc<CryptConfig>>,
    chunk_size: usize,
    wid: u64,
) -> Result<[u8;32], Error> {

    let mut zero_bytes = Vec::with_capacity(chunk_size);
    zero_bytes.resize(chunk_size, 0u8);
    let mut chunk_builder = DataChunkBuilder::new(&zero_bytes).compress(true);
    if let Some(ref crypt_config) = crypt_config {
        chunk_builder = chunk_builder.crypt_config(crypt_config);
    }
    let zero_chunk_digest = *chunk_builder.digest();

    let chunk = chunk_builder.build()?;
    let chunk_data = chunk.into_raw();

    let param = json!({
        "wid": wid,
        "digest": proxmox::tools::digest_to_hex(&zero_chunk_digest),
        "size": chunk_size,
        "encoded-size": chunk_data.len(),
    });

    client.upload_post("fixed_chunk", Some(param), "application/octet-stream", chunk_data).await?;

    Ok(zero_chunk_digest)
}

pub(crate) async fn add_config(
    client: Arc<BackupClient>,
    crypt_config: Option<Arc<CryptConfig>>,
    registry: Arc<Mutex<ImageRegistry>>,
    name: String,
    data: DataPointer,
    size: u64,
) -> Result<(), Error> {
    println!("add config {} size {}", name, size);

    let blob_name = format!("{}.blob", name);

    let data: &[u8] = unsafe { std::slice::from_raw_parts(data.0, size as usize) };
    let data = data.to_vec();

    let stats = client.upload_blob_from_data(data, &blob_name, crypt_config, true, false).await?;

    let mut guard = registry.lock().unwrap();
    guard.file_list.push(json!({
        "filename": blob_name,
        "size": stats.size,
        "csum": proxmox::tools::digest_to_hex(&stats.csum),
    }));

    Ok(())
}

pub(crate) async fn register_image(
    client: Arc<BackupClient>,
    crypt_config: Option<Arc<CryptConfig>>,
    registry: Arc<Mutex<ImageRegistry>>,
    known_chunks: Arc<Mutex<HashSet<[u8;32]>>>,
    device_name: String,
    device_size: u64,
    chunk_size: u64,
) -> Result<u8, Error> {
    println!("register image {} size {}", device_name, device_size);

    let archive_name = format!("{}.img.fidx", device_name);

    client.download_chunk_list("fixed_index", &archive_name, known_chunks.clone()).await?;
    println!("register image download chunk list OK");

    let param = json!({ "archive-name": archive_name , "size": device_size});
    let wid = client.post("fixed_index", Some(param)).await?.as_u64().unwrap();

    let zero_chunk_digest =
        register_zero_chunk(client.clone(), crypt_config, chunk_size as usize, wid).await?;

    let (upload_queue,  upload_result) = create_upload_queue(
        client.clone(),
        known_chunks.clone(),
        wid,
        device_size,
        chunk_size,
    );

    let info = ImageUploadInfo {
        wid,
        device_name,
        zero_chunk_digest,
        device_size,
        upload_queue: Some(upload_queue),
        upload_result: Some(upload_result),
   };

    let mut guard = registry.lock().unwrap();
    guard.register(info)
}

pub(crate) async fn close_image(
    client: Arc<BackupClient>,
    registry: Arc<Mutex<ImageRegistry>>,
    dev_id: u8,
) -> Result<(), Error> {

    println!("close image {}", dev_id);

    let (wid, upload_result, device_name, device_size) = {
        let mut guard = registry.lock().unwrap();
        let info = guard.lookup(dev_id)?;

        info.upload_queue.take(); // close

        (info.wid, info.upload_result.take(), info.device_name.clone(), info.device_size)
    };

    let upload_result = match upload_result {
        Some(upload_result) => {
            match upload_result.await? {
                Ok(res) => res,
                Err(err) => bail!("close_image: upload error: {}", err),
            }
        }
        None => bail!("close_image: unknown error because upload result channel was already closed"),
    };

    let param = json!({
        "wid": wid ,
        "chunk-count": upload_result.chunk_count,
        "size": upload_result.bytes_written,
        "csum": proxmox::tools::digest_to_hex(&upload_result.csum),
    });

    let _value = client.post("fixed_close", Some(param)).await?;

    let mut guard = registry.lock().unwrap();
    guard.file_list.push(json!({
        "filename": device_name,
        "size": device_size,
        "csum": proxmox::tools::digest_to_hex(&upload_result.csum),
    }));

    Ok(())
}

pub(crate) async fn write_data(
    client: Arc<BackupClient>,
    crypt_config: Option<Arc<CryptConfig>>,
    registry: Arc<Mutex<ImageRegistry>>,
    known_chunks: Arc<Mutex<HashSet<[u8;32]>>>,
    dev_id: u8,
    data: DataPointer,
    offset: u64,
    size: u64, // actual data size
    chunk_size: u64, // expected data size
) -> Result<(), Error> {

    println!("dev {}: write {} {}", dev_id, offset, size);

    let (wid, mut upload_queue, zero_chunk_digest, device_size) = {
        let mut guard = registry.lock().unwrap();
        let info = guard.lookup(dev_id)?;


        (info.wid, info.upload_queue.clone(), info.zero_chunk_digest, info.device_size)
    };

    // Note: last chunk may be smaller than chunk_size
    if size > chunk_size {
        bail!("write_data: got unexpected chunk size {}", size);
    }

    if offset & (chunk_size - 1) != 0 {
        bail!("write_data: offset {} is not correctly aligned", offset);
    }

    let end_offset = offset + chunk_size;
    if end_offset > device_size {
        bail!("write_data: write out of range");
    } if end_offset < device_size && size != chunk_size {
        bail!("write_data: chunk too small {}", size);
    }

    let upload_future: Box<dyn Future<Output = Result<ChunkUploadInfo, Error>> + Send + Unpin> = {
        if data.0 == ptr::null() {
            if size != chunk_size {
                bail!("write_data: got invalid null chunk");
            }
            let upload_info = ChunkUploadInfo { digest: zero_chunk_digest, offset, size, chunk_is_known: true };
            Box::new(futures::future::ok(upload_info))
        } else {
            let data: &[u8] = unsafe { std::slice::from_raw_parts(data.0, size as usize) };

            let mut chunk_builder = DataChunkBuilder::new(data).compress(true);

            if let Some(ref crypt_config) = crypt_config {
                chunk_builder = chunk_builder.crypt_config(crypt_config);
            }

            let digest = chunk_builder.digest();

            let chunk_is_known = {
                let known_chunks_guard = known_chunks.lock().unwrap();
                known_chunks_guard.contains(digest)
            };

            if chunk_is_known {
                let upload_info = ChunkUploadInfo { digest: *digest, offset, size, chunk_is_known: true };
                Box::new(futures::future::ok(upload_info))
           } else {
                let digest_copy = *digest;
                let digest_str = proxmox::tools::digest_to_hex(digest);
                let chunk = chunk_builder.build()?;
                let chunk_data = chunk.into_raw();

                let param = json!({
                    "wid": wid,
                    "digest": digest_str,
                    "size": size,
                    "encoded-size": chunk_data.len(),
                });

                // Phase 1: send data
                let response_future = client.send_upload_request(
                    "POST",
                    "fixed_chunk",
                    Some(param),
                    "application/octet-stream",
                    chunk_data,
                ).await?;

                // create response future (run that in other task)
                let upload_future = response_future
                    .map_err(Error::from)
                    .and_then(H2Client::h2api_response)
                    .map_ok(move |_| {
                        ChunkUploadInfo { digest: digest_copy, offset, size, chunk_is_known: false }
                    })
                    .map_err(|err| format_err!("pipelined request failed: {}", err));

                Box::new(Box::pin(upload_future))
            }
        }
    };

    match upload_queue {
        Some(ref mut upload_queue) => {
            // Phase 2: send reponse future to other task
            if let Err(_) = upload_queue.send(upload_future).await {
                let upload_result = {
                    let mut guard = registry.lock().unwrap();
                    let info = guard.lookup(dev_id)?;
                    info.upload_queue.take(); // close
                    info.upload_result.take()
                };
                match upload_result {
                    Some(upload_result) => {
                        match upload_result.await? {
                            Ok(res) => res,
                            Err(err) => bail!("write_data upload error: {}", err),
                        }
                    }
                    None => bail!("write_data: unknown error because upload result channel was already closed"),
                };
            }
        }
        None => {
            bail!("upload queue already closed");
        }
    }

    println!("upload chunk sucessful");

    Ok(())
}

pub(crate) async fn finish_backup(
    client: Arc<BackupClient>,
    registry: Arc<Mutex<ImageRegistry>>,
    setup: BackupSetup,
) -> Result<(), Error> {

    println!("call finish");

    let index_data = {
        let guard = registry.lock().unwrap();

        let index = json!({
            "backup-type": "vm",
            "backup-id": setup.backup_id,
            "backup-time": setup.backup_time.timestamp(),
            "files": &guard.file_list,
        });

        println!("Upload index.json");
        serde_json::to_string_pretty(&index)?.into()
    };

    client
        .upload_blob_from_data(index_data, "index.json.blob", setup.crypt_config.clone(), true, true)
        .await?;

    client.finish().await?;

    Ok(())
}
