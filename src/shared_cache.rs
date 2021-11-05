use once_cell::sync::OnceCell;
use pbs_tools::async_lru_cache::AsyncLruCache;
use std::sync::{Arc, Mutex};

type ChunkCache = AsyncLruCache<[u8; 32], Arc<Vec<u8>>>;

const SHARED_CACHE_CAPACITY: usize = 64; // 256 MB
static SHARED_CACHE: OnceCell<Mutex<Option<Arc<ChunkCache>>>> = OnceCell::new();

pub fn get_shared_chunk_cache() -> Arc<ChunkCache> {
    let mut guard = SHARED_CACHE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap();
    match &*guard {
        Some(cache) => Arc::clone(cache),
        None => {
            let cache = Arc::new(AsyncLruCache::new(SHARED_CACHE_CAPACITY));
            *guard = Some(Arc::clone(&cache));
            cache
        }
    }
}

pub fn shared_chunk_cache_cleanup() {
    let mut guard = SHARED_CACHE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap();
    if let Some(arc) = guard.as_ref() {
        let refcount = Arc::strong_count(arc);
        if refcount == 1 {
            // no one else is using the cache anymore, drop it
            let _drop = guard.take();
        }
    }
}
