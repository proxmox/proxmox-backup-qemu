use anyhow::Error;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use futures::future::Future;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use pbs_client::*;
use pbs_datastore::fixed_index::FixedIndexReader;
use pbs_datastore::index::IndexFile;

pub(crate) struct ChunkUploadInfo {
    pub digest: [u8; 32],
    pub chunk_is_known: bool,
    pub offset: u64,
    pub size: u64,
}

pub(crate) struct UploadResult {
    pub csum: [u8; 32],
    pub chunk_count: u64,
    pub bytes_written: u64,
}

pub(crate) type UploadQueueSender =
    mpsc::Sender<Box<dyn Future<Output = Result<ChunkUploadInfo, Error>> + Send + Unpin>>;
type UploadQueueReceiver =
    mpsc::Receiver<Box<dyn Future<Output = Result<ChunkUploadInfo, Error>> + Send + Unpin>>;
pub(crate) type UploadResultReceiver = oneshot::Receiver<Result<UploadResult, Error>>;
type UploadResultSender = oneshot::Sender<Result<UploadResult, Error>>;

pub(crate) fn create_upload_queue(
    client: Arc<BackupWriter>,
    known_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
    initial_index: Arc<Option<FixedIndexReader>>,
    wid: u64,
    device_size: u64,
    chunk_size: u64,
) -> (UploadQueueSender, UploadResultReceiver) {
    let (upload_queue_tx, upload_queue_rx) = mpsc::channel(100);
    let (upload_result_tx, upload_result_rx) = oneshot::channel();

    tokio::spawn(upload_handler(
        client,
        known_chunks,
        initial_index,
        wid,
        device_size,
        chunk_size,
        upload_queue_rx,
        upload_result_tx,
    ));

    (upload_queue_tx, upload_result_rx)
}

async fn upload_chunk_list(
    client: Arc<BackupWriter>,
    wid: u64,
    digest_list: &mut Vec<String>,
    offset_list: &mut Vec<u64>,
) -> Result<(), Error> {
    let param = json!({ "wid": wid, "digest-list": digest_list, "offset-list": offset_list });
    let param_data = param.to_string().as_bytes().to_vec();

    digest_list.truncate(0);
    offset_list.truncate(0);

    client
        .upload_put("fixed_index", None, "application/json", param_data)
        .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::needless_range_loop)]
async fn upload_handler(
    client: Arc<BackupWriter>,
    known_chunks: Arc<Mutex<HashSet<[u8; 32]>>>,
    initial_index: Arc<Option<FixedIndexReader>>,
    wid: u64,
    device_size: u64,
    chunk_size: u64,
    mut upload_queue: UploadQueueReceiver,
    upload_result: UploadResultSender,
) {
    let mut chunk_count = 0;
    let mut bytes_written = 0;

    let mut digest_list = Vec::new();
    let mut offset_list = Vec::new();

    let index_size = ((device_size + chunk_size - 1) / chunk_size) as usize;
    let mut index = Vec::with_capacity(index_size);
    index.resize(index_size, [0u8; 32]);

    // for incremental, initialize with data from previous backup
    // caller ensures initial_index length is index_size
    if let Some(init) = initial_index.as_ref() {
        for i in 0..index_size {
            index[i] = *init.index_digest(i).unwrap();
        }
    }

    while let Some(response_future) = upload_queue.recv().await {
        match response_future.await {
            Ok(ChunkUploadInfo {
                digest,
                offset,
                size,
                chunk_is_known,
            }) => {
                let digest_str = hex::encode(digest);

                //println!("upload_handler {:?} {}", digest, offset);
                let pos = (offset / chunk_size) as usize;
                index[pos] = digest;

                chunk_count += 1;
                bytes_written += size;

                if !chunk_is_known {
                    // register chunk as known
                    let mut known_chunks_guard = known_chunks.lock().unwrap();
                    known_chunks_guard.insert(digest);
                }

                digest_list.push(digest_str);
                offset_list.push(offset);

                if digest_list.len() >= 128 {
                    if let Err(err) = upload_chunk_list(
                        Arc::clone(&client),
                        wid,
                        &mut digest_list,
                        &mut offset_list,
                    )
                    .await
                    {
                        let _ = upload_result.send(Err(err));
                        return;
                    }
                }
            }
            Err(err) => {
                let _ = upload_result.send(Err(err));
                return;
            }
        }
    }

    if !digest_list.is_empty() {
        if let Err(err) =
            upload_chunk_list(Arc::clone(&client), wid, &mut digest_list, &mut offset_list).await
        {
            let _ = upload_result.send(Err(err));
            return;
        }
    }

    let mut csum = openssl::sha::Sha256::new();
    for digest in index.iter() {
        csum.update(digest);
    }
    let csum = csum.finish();

    let _ = upload_result.send(Ok(UploadResult {
        csum,
        chunk_count,
        bytes_written,
    }));
}
