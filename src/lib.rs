use failure::*;
use std::collections::HashSet;
use std::thread::JoinHandle;
use std::sync::{Mutex, Arc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::ffi::{CStr, CString};
use std::ptr;
use std::os::raw::{c_char, c_int, c_void};

use serde_json::json;
use futures::future::{Future, Either, FutureExt};
use tokio::runtime::Runtime;

//#[macro_use]
use proxmox_backup::backup::*;
use proxmox_backup::client::*;
use proxmox_backup::tools::BroadcastFuture;

use chrono::{Utc, TimeZone, DateTime};

struct BackupRepository {
    host: String,
    store: String,
    user: String,
    chunk_size: u64,
    backup_id: String,
    backup_time: DateTime<Utc>,
    password: String,
    crypt_config: Option<Arc<CryptConfig>>,
}

struct BackupTask {
    worker: JoinHandle<Result<BackupStats, Error>>,
    command_tx: Sender<BackupMessage>,
    aborted: Option<String>,  // set on abort, conatins abort reason
}

#[derive(Debug)]
struct BackupStats {
    written_bytes: u64,
}

struct CallbackPointers {
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    error: * mut *mut c_char,
}
unsafe impl std::marker::Send for CallbackPointers {}

struct DataPointer (*const u8);
unsafe impl std::marker::Send for DataPointer {}

enum BackupMessage {
    End,
    Abort,
    RegisterImage {
        device_name: String,
        size: u64,
        result_channel: Arc<Mutex<Sender<Result<u8, Error>>>>,
    },
    CloseImage {
        dev_id: u8,
        callback_info: CallbackPointers,
    },
    WriteData {
        dev_id: u8,
        data: DataPointer,
        offset: u64,
        size: u64,
        callback_info: CallbackPointers,
    },
    Finish {
        callback_info: CallbackPointers,
    },
}

struct ImageUploadInfo {
    wid: u64,
    known_chunks: Arc<Mutex<HashSet<[u8;32]>>>,
    zero_chunk_digest_str: String,
    digest_list: Vec<String>,
    offset_list: Vec<u64>,
    size: u64,
    written: u64,
    chunk_count: u64,
}

struct ImageRegistry {
    upload_info: Vec<ImageUploadInfo>,
}

impl ImageRegistry {

    fn new() -> Self {
        Self {
            upload_info: Vec::new(),
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
) -> Result<String, Error> {

    let mut zero_bytes = Vec::with_capacity(chunk_size);
    zero_bytes.resize(chunk_size, 0u8);
    let mut chunk_builder = DataChunkBuilder::new(&zero_bytes).compress(true);
    if let Some(ref crypt_config) = crypt_config {
        chunk_builder = chunk_builder.crypt_config(crypt_config);
    }
    let zero_chunk_digest_str = proxmox::tools::digest_to_hex(chunk_builder.digest());

    let chunk = chunk_builder.build()?;
    let chunk_data = chunk.into_raw();

    let param = json!({
        "wid": wid,
        "digest": zero_chunk_digest_str,
        "size": chunk_size,
        "encoded-size": chunk_data.len(),
    });

    client.upload_post("fixed_chunk", Some(param), "application/octet-stream", chunk_data).await?;

    Ok(zero_chunk_digest_str)
}

async fn register_image(
    client: Arc<BackupClient>,
    crypt_config: Option<Arc<CryptConfig>>,
    registry: Arc<Mutex<ImageRegistry>>,
    known_chunks: Arc<Mutex<HashSet<[u8;32]>>>,
    device_name: String,
    size: u64,
    chunk_size: u64,
) -> Result<u8, Error> {
    println!("register image {} size {}", device_name, size);

    let archive_name = format!("{}.img.fidx", device_name);

    client.download_chunk_list("fixed_index", &archive_name, known_chunks.clone()).await?;
    println!("register image download chunk list OK");

    let param = json!({ "archive-name": archive_name , "size": size});
    let wid = client.post("fixed_index", Some(param)).await?.as_u64().unwrap();

    let zero_chunk_digest_str = register_zero_chunk(client, crypt_config, chunk_size as usize, wid).await?;

    let info = ImageUploadInfo {
        wid,
        known_chunks,
        zero_chunk_digest_str,
        size,
        digest_list: Vec::new(),
        offset_list: Vec::new(),
        written: 0,
        chunk_count: 0,
    };

    let mut guard = registry.lock().unwrap();
    guard.register(info)
}

async fn close_image(
    client: Arc<BackupClient>,
    registry: Arc<Mutex<ImageRegistry>>,
    dev_id: u8,
) -> Result<(), Error> {

    println!("close image {}", dev_id);

    let (wid, written, chunk_count, append_list) = {
        let mut guard = registry.lock().unwrap();
        let info = guard.lookup(dev_id)?;

        let append_list = if info.digest_list.len() > 0 {
            let param = json!({ "wid": info.wid, "digest-list": info.digest_list, "offset-list": info.offset_list });
            let param_data = param.to_string().as_bytes().to_vec();
            info.digest_list.truncate(0);
            info.offset_list.truncate(0);
            Some(param_data)
        } else {
            None
        };

        (info.wid, info.written, info.chunk_count, append_list)
    };

    if let Some(data) = append_list {
        client.upload_put("fixed_index", None, "application/json", data).await?;
    }

    let param = json!({
        "wid": wid ,
        "chunk-count": chunk_count,
        "size": written,
    });

    let _value = client.post("fixed_close", Some(param)).await?;

    Ok(())
}

async fn write_data(
    client: Arc<BackupClient>,
    crypt_config: Option<Arc<CryptConfig>>,
    registry: Arc<Mutex<ImageRegistry>>,
    dev_id: u8,
    data: DataPointer,
    offset: u64,
    size: u64, // actual data size
    chunk_size: u64, // expected data size
) -> Result<(), Error> {

    println!("dev {}: write {}", dev_id, size);

    if size > chunk_size {
        bail!("write_data: got unexpected chunk size {}", size);
    }

    let (wid, known_chunks, zero_chunk_digest_str) = {
        let mut guard = registry.lock().unwrap();
        let info = guard.lookup(dev_id)?;
        let zero_chunk_digest_str = if data.0 == ptr::null() {
            Some(info.zero_chunk_digest_str.clone())
        } else {
            None
        };
        (info.wid, info.known_chunks.clone(),zero_chunk_digest_str)
    };

    println!("write_data 1 wid = {}, {:p}", wid, data.0);

    let digest_str = if data.0 == ptr::null() {
        if let Some(digest_str) = zero_chunk_digest_str {
            digest_str
        } else {
            unreachable!();
        }
    } else {
        let data: &[u8] = unsafe { std::slice::from_raw_parts(data.0, size as usize) };

        let mut chunk_builder = DataChunkBuilder::new(data).compress(true);

        if let Some(ref crypt_config) = crypt_config {
            chunk_builder = chunk_builder.crypt_config(crypt_config);
        }

        let digest = chunk_builder.digest();
        let digest_str = proxmox::tools::digest_to_hex(digest);

        let chunk_is_known = known_chunks.lock().unwrap().contains(digest);

        if chunk_is_known {
            digest_str
        } else {
            let digest = *digest;
            let chunk = chunk_builder.build()?;
            let chunk_data = chunk.into_raw();

            let param = json!({
                "wid": wid,
                "digest": digest_str,
                "size": size,
                "encoded-size": chunk_data.len(),
            });

            client.upload_post("fixed_chunk", Some(param), "application/octet-stream", chunk_data).await?;
            known_chunks.lock().unwrap().insert(digest);
            digest_str
        }
    };

    let append_list = {
        let mut guard = registry.lock().unwrap();
        let mut info = guard.lookup(dev_id)?;
        info.written += size;
        info.chunk_count += 1;

        info.digest_list.push(digest_str);
        info.offset_list.push(offset);

        if info.digest_list.len() >= 128 {
            let param = json!({ "wid": wid, "digest-list": info.digest_list, "offset-list": info.offset_list });
            let param_data = param.to_string().as_bytes().to_vec();
            info.digest_list.truncate(0);
            info.offset_list.truncate(0);
            Some(param_data)
        } else {
            None
        }
    };

    if let Some(data) = append_list {
        client.upload_put("fixed_index", None, "application/json", data).await?;
    }

    println!("upload chunk sucessful");

    Ok(())
}

async fn finish_backup(
    client: Arc<BackupClient>,
    _registry: Arc<Mutex<ImageRegistry>>,
) -> Result<(), Error> {

    println!("call finish");
    client.finish().await?;

    Ok(())
}

impl BackupTask {

    fn new(repo: BackupRepository) -> Result<Self, Error> {

        let (connect_tx, connect_rx) = channel(); // sync initial server connect

        let (command_tx, command_rx) = channel();

        let worker = std::thread::spawn(move ||  {
            backup_worker_task(repo, connect_tx, command_rx)
        });

        connect_rx.recv().unwrap()?;

        Ok(BackupTask {
            worker,
            command_tx,
            aborted: None,
        })
    }
}

fn connect(runtime: &mut Runtime, repo: &BackupRepository) -> Result<Arc<BackupClient>, Error> {
    let client = HttpClient::new(&repo.host, &repo.user, Some(repo.password.clone()))?;

    let client = runtime.block_on(
        client.start_backup(&repo.store, "vm", &repo.backup_id, repo.backup_time, false))?;

    Ok(client)
}

fn handle_async_command<F: 'static + Send + Future<Output=Result<(), Error>>>(
    command_future: F,
    abort_future: impl 'static + Send + Future<Output=Result<(), Error>>,
    callback_info: CallbackPointers,
) -> impl Future<Output = ()> {

    futures::future::select(command_future.boxed(), abort_future.boxed())
        .map(move |either| {
            match either {
                Either::Left((result, _)) => {
                    match result {
                        Ok(_) => {
                            println!("command sucessful");
                            unsafe { *(callback_info.error) = ptr::null_mut(); }
                            (callback_info.callback)(callback_info.callback_data);
                        }
                        Err(err) => {
                            println!("command error {}", err);
                            let errmsg = CString::new(format!("command error: {}", err)).unwrap();
                            unsafe { *(callback_info.error) = errmsg.into_raw(); }
                            (callback_info.callback)(callback_info.callback_data);
                        }
                    }
                }
                Either::Right(_) => { // aborted
                    println!("command aborted");
                    let errmsg = CString::new("copmmand aborted".to_string()).unwrap();
                    unsafe { *(callback_info.error) = errmsg.into_raw(); }
                    (callback_info.callback)(callback_info.callback_data);
                }
            }
        })
}

fn backup_worker_task(
    repo: BackupRepository,
    connect_tx: Sender<Result<(), Error>>,
    command_rx: Receiver<BackupMessage>,
) -> Result<BackupStats, Error>  {

    let mut builder = tokio::runtime::Builder::new();

    builder.blocking_threads(1);
    builder.core_threads(4);
    builder.name_prefix("proxmox-backup-qemu-");

    let mut runtime = match builder.build() {
        Ok(runtime) => runtime,
        Err(err) =>  {
            connect_tx.send(Err(format_err!("create runtime failed: {}", err))).unwrap();
            bail!("create runtiome failed");
        }
    };

    let client = match connect(&mut runtime, &repo) {
        Ok(client) => {
            connect_tx.send(Ok(())).unwrap();
            client
        }
        Err(err) => {
            connect_tx.send(Err(err)).unwrap();
            bail!("connection failed");
        }
    };

    drop(connect_tx); // no longer needed

    let (mut abort_tx, mut abort_rx) = tokio::sync::mpsc::channel(1);
    let abort_rx = async move {
        match abort_rx.recv().await {
            Some(()) => Ok(()),
            None => bail!("abort future canceled"),
        }
    };

    let abort = BroadcastFuture::new(Box::new(abort_rx));

    let written_bytes = Arc::new(AtomicU64::new(0));
    let written_bytes2 = written_bytes.clone();

    let known_chunks = Arc::new(Mutex::new(HashSet::new()));
    let crypt_config = repo.crypt_config;
    let chunk_size = repo.chunk_size;

    runtime.spawn(async move  {

        let registry = Arc::new(Mutex::new(ImageRegistry::new()));

        loop {
            let msg = command_rx.recv().unwrap(); // todo: should be blocking

            match msg {
                BackupMessage::Abort => {
                    println!("worker got abort mesage");
                    let res = abort_tx.send(()).await;
                    if let Err(_err) = res  {
                        println!("sending abort failed");
                    }
                }
                BackupMessage::End => {
                    println!("worker got end mesage");
                    break;
                }
                BackupMessage::RegisterImage { device_name, size, result_channel } => {
                    let res = register_image(
                        client.clone(),
                        crypt_config.clone(),
                        registry.clone(),
                        known_chunks.clone(),
                        device_name,
                        size,
                        chunk_size,
                    ).await;
                    let _ = result_channel.lock().unwrap().send(res);
                }
                BackupMessage::CloseImage { dev_id, callback_info } => {
                    handle_async_command(
                        close_image(client.clone(), registry.clone(), dev_id),
                        abort.listen(),
                        callback_info,
                    ).await;
                }
                BackupMessage::WriteData { dev_id, data, offset, size, callback_info } => {
                    written_bytes2.fetch_add(size, Ordering::SeqCst);

                    handle_async_command(
                        write_data(
                            client.clone(),
                            crypt_config.clone(),
                            registry.clone(),
                            dev_id, data,
                            offset,
                            size,
                            chunk_size,
                        ),
                        abort.listen(),
                        callback_info,
                    ).await;
                }
                BackupMessage::Finish { callback_info } => {
                    handle_async_command(
                        finish_backup(client.clone(), registry.clone()),
                        abort.listen(),
                        callback_info,
                    ).await;
                }
            }
        }

        println!("worker end loop");
    });

    runtime.shutdown_on_idle();

    let stats = BackupStats { written_bytes: written_bytes.fetch_add(0,Ordering::SeqCst)  };
    Ok(stats)
}

// The C interface

#[repr(C)]
pub struct ProxmoxBackupHandle;

#[no_mangle]
pub extern "C" fn proxmox_backup_free_error(ptr: * mut c_char) {
    unsafe { CString::from_raw(ptr); }
}

macro_rules! raise_error_null {
    ($error:ident, $err:expr) => {{
        let errmsg = CString::new($err.to_string()).unwrap(); // fixme
        unsafe { *$error =  errmsg.into_raw(); }
        return ptr::null_mut();
    }}
}

macro_rules! raise_error_int {
    ($error:ident, $err:expr) => {{
        let errmsg = CString::new($err.to_string()).unwrap(); // fixme
        unsafe { *$error =  errmsg.into_raw(); }
        return -1 as c_int;
    }}
}

#[no_mangle]
pub extern "C" fn proxmox_backup_connect(error: * mut * mut c_char) -> *mut ProxmoxBackupHandle {

    println!("Hello");

    let backup_time = Utc.timestamp(Utc::now().timestamp(), 0);

    let crypt_config: Option<Arc<CryptConfig>> = None;

    let repo = BackupRepository {
        host: "localhost".to_owned(),
        user: "root@pam".to_owned(),
        store: "store2".to_owned(),
        chunk_size: 4*1024*1024,
        backup_id: "99".to_owned(),
        password: "12345".to_owned(),
        backup_time,
        crypt_config,
    };

    match BackupTask::new(repo) {
        Ok(task) => {
            let boxed_task = Box::new(task);
            Box::into_raw(boxed_task) as * mut ProxmoxBackupHandle
        }
        Err(err) => raise_error_null!(error, err),
    }
}

#[no_mangle]
pub extern "C" fn proxmox_backup_abort(
    handle: *mut ProxmoxBackupHandle,
    reason: *const c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    let reason = unsafe { CStr::from_ptr(reason).to_string_lossy().into_owned() };
    task.aborted = Some(reason);

    println!("send abort");
    let _res = task.command_tx.send(BackupMessage::Abort);
}

#[no_mangle]
pub extern "C" fn proxmox_backup_register_image(
    handle: *mut ProxmoxBackupHandle,
    device_name: *const c_char, // expect utf8 here
    size: u64,
    error: * mut * mut c_char,
) -> c_int {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        raise_error_int!(error, "task already aborted");
    }

    let device_name = unsafe { CStr::from_ptr(device_name).to_string_lossy().to_string() };

    let (result_sender, result_receiver) = channel();

    let msg = BackupMessage::RegisterImage {
        device_name,
        size,
        result_channel: Arc::new(Mutex::new(result_sender)),
    };

    println!("register_image_async start");
    let _res = task.command_tx.send(msg); // fixme: log errors
    println!("register_image_async send end");

    match result_receiver.recv() {
        Err(err) => raise_error_int!(error, format!("channel recv error: {}", err)),
        Ok(Err(err)) => raise_error_int!(error, err.to_string()),
        Ok(Ok(result)) => result as c_int,
    }
}

#[no_mangle]
pub extern "C" fn proxmox_backup_write_data_async(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    data: *const u8,
    offset: u64,
    size: u64,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        callback(callback_data);
        return;
    }

    let msg = BackupMessage::WriteData {
        dev_id,
        data: DataPointer(data),
        offset,
        size,
        callback_info: CallbackPointers { callback, callback_data, error },
    };

    println!("write_data_async start");
    let _res = task.command_tx.send(msg); // fixme: log errors
    println!("write_data_async end");
}

#[no_mangle]
pub extern "C" fn proxmox_backup_close_image_async(
    handle: *mut ProxmoxBackupHandle,
    dev_id: u8,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        callback(callback_data);
        return;
    }

    let msg = BackupMessage::CloseImage {
        dev_id,
        callback_info: CallbackPointers { callback, callback_data, error },
    };

    println!("close_image_async start");
    let _res = task.command_tx.send(msg); // fixme: log errors
    println!("close_image_async end");
}

#[no_mangle]
pub extern "C" fn proxmox_backup_finish_async(
    handle: *mut ProxmoxBackupHandle,
    callback: extern "C" fn(*mut c_void),
    callback_data: *mut c_void,
    error: * mut * mut c_char,
) {
    let task = unsafe { &mut *(handle as * mut BackupTask) };

    if let Some(_reason) = &task.aborted {
        let errmsg = CString::new("task already aborted".to_string()).unwrap();
        unsafe { *error =  errmsg.into_raw(); }
        callback(callback_data);
        return;
    }

    let msg = BackupMessage::Finish {
        callback_info: CallbackPointers { callback, callback_data, error },
    };

    println!("finish_async start");
    let _res = task.command_tx.send(msg); // fixme: log errors
    println!("finish_async end");
}

#[no_mangle]
pub extern "C" fn proxmox_backup_disconnect(handle: *mut ProxmoxBackupHandle) {

    println!("diconnect");

    let task = handle as * mut BackupTask;
    let task = unsafe { Box::from_raw(task) }; // take ownership

    println!("send end");
    let _res = task.command_tx.send(BackupMessage::End); // fixme: log errors

    println!("try join");
    match task.worker.join() {
        Ok(result) => {
            match result {
                Ok(stats) => {
                    println!("worker finished {:?}", stats);
                }
                Err(err) => {
                    println!("worker finished with error: {:?}", err);
                }
            }
        }
        Err(err) => {
           println!("worker paniced with error: {:?}", err);
        }
    }

    //drop(task);
}
