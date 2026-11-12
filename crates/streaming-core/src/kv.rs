//! The key-value store contract.
//!
//! The command set is deliberately the intersection of what Redis and
//! Clever Cloud Materia KV offer: single-command atomicity only, no `MULTI`,
//! no scripting. Exactly-once is built on [`Kv::hsetnx`] alone, used as a
//! compare-and-set on a version number.
//!
//! Every error must be treated as *outcome unknown*: the command may or may
//! not have been applied. Callers are written to be safe under blind retry.

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

/// A KV failure. The outcome of the failed command is unknown.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KvError {
    /// The store could not be reached or did not answer in time.
    #[error("kv unavailable: {0}")]
    Unavailable(String),
    /// The store answered something unexpected (wrong type, malformed reply).
    #[error("kv protocol error: {0}")]
    Protocol(String),
    /// A value exceeds what the store accepts.
    #[error("kv value too large: {size} bytes, limit {limit}")]
    TooLarge {
        /// Size of the rejected value.
        size: usize,
        /// The store's limit.
        limit: usize,
    },
    /// A stored document could not be decoded.
    #[error("kv document `{key}` is malformed: {reason}")]
    Malformed {
        /// Key of the document.
        key: String,
        /// Decoder message.
        reason: String,
    },
}

/// Shared handle on a KV store.
pub type DynKv = Arc<dyn Kv>;

/// The commands the framework needs. Each one is atomic on its own.
#[async_trait]
pub trait Kv: Send + Sync + 'static {
    /// `GET key`.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError>;
    /// `SET key value`.
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), KvError>;
    /// `DEL key [key ...]`. Missing keys are ignored.
    async fn del(&self, keys: Vec<String>) -> Result<(), KvError>;
    /// `INCR key`, returning the new value.
    async fn incr(&self, key: &str) -> Result<i64, KvError>;
    /// `HSETNX key field value`. Returns `true` when the field was created,
    /// `false` when it already existed (and was left untouched).
    async fn hsetnx(&self, key: &str, field: &str, value: Vec<u8>) -> Result<bool, KvError>;
    /// `HSET key field value [field value ...]`, atomically.
    async fn hset(&self, key: &str, entries: Vec<(String, Vec<u8>)>) -> Result<(), KvError>;
    /// `HGET key field`.
    async fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>, KvError>;
    /// Every field of a hash. Implementations may page through `HSCAN`.
    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>, KvError>;
    /// `HDEL key field [field ...]`. Missing fields are ignored.
    async fn hdel(&self, key: &str, fields: Vec<String>) -> Result<(), KvError>;
}

impl dyn Kv {
    /// `GET` then decode JSON.
    pub async fn get_json<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, KvError> {
        match self.get(key).await? {
            Some(bytes) => decode_json(key, &bytes).map(Some),
            None => Ok(None),
        }
    }

    /// Encode JSON then `SET`.
    pub async fn set_json<T: Serialize + Sync>(&self, key: &str, value: &T) -> Result<(), KvError> {
        self.set(key, encode_json(key, value)?).await
    }

    /// `HGET` then decode JSON.
    pub async fn hget_json<T: DeserializeOwned>(&self, key: &str, field: &str) -> Result<Option<T>, KvError> {
        match self.hget(key, field).await? {
            Some(bytes) => decode_json(key, &bytes).map(Some),
            None => Ok(None),
        }
    }

    /// Encode JSON then `HSET` one field.
    pub async fn hset_json<T: Serialize + Sync>(&self, key: &str, field: &str, value: &T) -> Result<(), KvError> {
        self.hset(key, vec![(field.to_owned(), encode_json(key, value)?)]).await
    }

    /// `HGETALL` then decode every value as JSON. Undecodable fields are skipped.
    pub async fn hgetall_json<T: DeserializeOwned>(&self, key: &str) -> Result<Vec<(String, T)>, KvError> {
        Ok(self
            .hgetall(key)
            .await?
            .into_iter()
            .filter_map(|(field, bytes)| serde_json::from_slice(&bytes).ok().map(|value| (field, value)))
            .collect())
    }
}

/// Decode a JSON document read from `key`.
pub fn decode_json<T: DeserializeOwned>(key: &str, bytes: &[u8]) -> Result<T, KvError> {
    serde_json::from_slice(bytes).map_err(|e| KvError::Malformed {
        key: key.to_owned(),
        reason: e.to_string(),
    })
}

/// Encode a JSON document destined to `key`.
pub fn encode_json<T: Serialize>(key: &str, value: &T) -> Result<Vec<u8>, KvError> {
    serde_json::to_vec(value).map_err(|e| KvError::Malformed {
        key: key.to_owned(),
        reason: e.to_string(),
    })
}

#[derive(Debug, Clone)]
enum Entry {
    Str(Vec<u8>),
    Hash(BTreeMap<String, Vec<u8>>),
}

/// Counters describing the traffic an in-memory store has served.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemKvStats {
    /// Number of read commands.
    pub reads: u64,
    /// Number of write commands.
    pub writes: u64,
    /// Number of keys currently stored.
    pub keys: u64,
    /// Approximate payload size currently stored, in bytes.
    pub bytes: u64,
}

/// An in-memory [`Kv`], for standalone mode and tests.
///
/// Semantics follow Redis: a key holds either a string or a hash, using the
/// wrong command on a key is a protocol error, and a hash that loses its last
/// field disappears.
#[derive(Debug, Default)]
pub struct MemKv {
    entries: Mutex<BTreeMap<String, Entry>>,
    reads: AtomicU64,
    writes: AtomicU64,
}

impl MemKv {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store behind a shared handle.
    #[must_use]
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Traffic and size counters.
    #[must_use]
    pub fn stats(&self) -> MemKvStats {
        let entries = self.entries.lock();
        let bytes = entries
            .iter()
            .map(|(key, entry)| {
                key.len()
                    + match entry {
                        Entry::Str(value) => value.len(),
                        Entry::Hash(fields) => fields.iter().map(|(f, v)| f.len() + v.len()).sum(),
                    }
            })
            .sum::<usize>();
        MemKvStats {
            reads: self.reads.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            keys: entries.len() as u64,
            bytes: bytes as u64,
        }
    }

    /// Every key currently stored, in order. Meant for tests and debugging.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.entries.lock().keys().cloned().collect()
    }

    fn read(&self) {
        self.reads.fetch_add(1, Ordering::Relaxed);
    }

    fn write(&self) {
        self.writes.fetch_add(1, Ordering::Relaxed);
    }
}

fn wrong_type(key: &str) -> KvError {
    KvError::Protocol(format!(
        "WRONGTYPE operation against key `{key}` holding the wrong kind of value"
    ))
}

#[async_trait]
impl Kv for MemKv {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        self.read();
        match self.entries.lock().get(key) {
            None => Ok(None),
            Some(Entry::Str(value)) => Ok(Some(value.clone())),
            Some(Entry::Hash(_)) => Err(wrong_type(key)),
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), KvError> {
        self.write();
        self.entries.lock().insert(key.to_owned(), Entry::Str(value));
        Ok(())
    }

    async fn del(&self, keys: Vec<String>) -> Result<(), KvError> {
        self.write();
        let mut entries = self.entries.lock();
        for key in keys {
            entries.remove(&key);
        }
        Ok(())
    }

    async fn incr(&self, key: &str) -> Result<i64, KvError> {
        self.write();
        let mut entries = self.entries.lock();
        let current = match entries.get(key) {
            None => 0,
            Some(Entry::Str(value)) => std::str::from_utf8(value)
                .ok()
                .and_then(|text| text.parse::<i64>().ok())
                .ok_or_else(|| KvError::Protocol(format!("value at `{key}` is not an integer")))?,
            Some(Entry::Hash(_)) => return Err(wrong_type(key)),
        };
        let next = current
            .checked_add(1)
            .ok_or_else(|| KvError::Protocol("increment or decrement would overflow".to_owned()))?;
        entries.insert(key.to_owned(), Entry::Str(next.to_string().into_bytes()));
        Ok(next)
    }

    async fn hsetnx(&self, key: &str, field: &str, value: Vec<u8>) -> Result<bool, KvError> {
        self.write();
        let mut entries = self.entries.lock();
        match entries
            .entry(key.to_owned())
            .or_insert_with(|| Entry::Hash(BTreeMap::new()))
        {
            Entry::Hash(fields) => {
                if fields.contains_key(field) {
                    Ok(false)
                } else {
                    fields.insert(field.to_owned(), value);
                    Ok(true)
                }
            }
            Entry::Str(_) => Err(wrong_type(key)),
        }
    }

    async fn hset(&self, key: &str, new_fields: Vec<(String, Vec<u8>)>) -> Result<(), KvError> {
        self.write();
        if new_fields.is_empty() {
            return Ok(());
        }
        let mut entries = self.entries.lock();
        match entries
            .entry(key.to_owned())
            .or_insert_with(|| Entry::Hash(BTreeMap::new()))
        {
            Entry::Hash(fields) => {
                fields.extend(new_fields);
                Ok(())
            }
            Entry::Str(_) => Err(wrong_type(key)),
        }
    }

    async fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>, KvError> {
        self.read();
        match self.entries.lock().get(key) {
            None => Ok(None),
            Some(Entry::Hash(fields)) => Ok(fields.get(field).cloned()),
            Some(Entry::Str(_)) => Err(wrong_type(key)),
        }
    }

    async fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>, KvError> {
        self.read();
        match self.entries.lock().get(key) {
            None => Ok(Vec::new()),
            Some(Entry::Hash(fields)) => Ok(fields
                .iter()
                .map(|(field, value)| (field.clone(), value.clone()))
                .collect()),
            Some(Entry::Str(_)) => Err(wrong_type(key)),
        }
    }

    async fn hdel(&self, key: &str, removed: Vec<String>) -> Result<(), KvError> {
        self.write();
        let mut entries = self.entries.lock();
        let now_empty = match entries.get_mut(key) {
            None => false,
            Some(Entry::Hash(fields)) => {
                for field in &removed {
                    fields.remove(field);
                }
                fields.is_empty()
            }
            Some(Entry::Str(_)) => return Err(wrong_type(key)),
        };
        if now_empty {
            entries.remove(key);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn hsetnx_is_a_compare_and_set() {
        block_on(async {
            let kv = MemKv::new();
            assert!(kv.hsetnx("log", "c:1", b"first".to_vec()).await.unwrap());
            assert!(!kv.hsetnx("log", "c:1", b"second".to_vec()).await.unwrap());
            assert_eq!(kv.hget("log", "c:1").await.unwrap().unwrap(), b"first");
        });
    }

    #[test]
    fn redis_like_semantics() {
        block_on(async {
            let kv = MemKv::new();
            assert_eq!(kv.incr("epoch").await.unwrap(), 1);
            assert_eq!(kv.incr("epoch").await.unwrap(), 2);
            kv.set("s", b"x".to_vec()).await.unwrap();
            assert!(kv.hget("s", "f").await.is_err());
            kv.hset("h", vec![("a".into(), b"1".to_vec()), ("b".into(), b"2".to_vec())])
                .await
                .unwrap();
            assert_eq!(kv.hgetall("h").await.unwrap().len(), 2);
            kv.hdel("h", vec!["a".into(), "b".into(), "zzz".into()]).await.unwrap();
            assert!(!kv.keys().contains(&"h".to_owned()));
            kv.del(vec!["s".into(), "missing".into()]).await.unwrap();
            assert_eq!(kv.get("s").await.unwrap(), None);
        });
    }

    #[test]
    fn json_helpers() {
        block_on(async {
            let kv: DynKv = MemKv::shared();
            kv.set_json("doc", &vec![1, 2, 3]).await.unwrap();
            assert_eq!(kv.get_json::<Vec<i32>>("doc").await.unwrap(), Some(vec![1, 2, 3]));
            kv.set("bad", b"{".to_vec()).await.unwrap();
            assert!(matches!(
                kv.get_json::<Vec<i32>>("bad").await,
                Err(KvError::Malformed { .. })
            ));
        });
    }

    #[test]
    fn hash_json_helpers() {
        block_on(async {
            let kv: DynKv = MemKv::shared();
            assert_eq!(kv.hget_json::<u32>("h", "a").await.unwrap(), None);
            kv.hset_json("h", "a", &1_u32).await.unwrap();
            kv.hset_json("h", "b", &2_u32).await.unwrap();
            kv.hset("h", vec![("bad".into(), b"{".to_vec())]).await.unwrap();
            assert_eq!(kv.hget_json::<u32>("h", "b").await.unwrap(), Some(2));
            // Undecodable fields are skipped.
            assert_eq!(
                kv.hgetall_json::<u32>("h").await.unwrap(),
                vec![("a".to_owned(), 1), ("b".to_owned(), 2)]
            );
        });
    }

    #[test]
    fn stats_count_traffic_keys_and_bytes() {
        block_on(async {
            let kv = MemKv::shared();
            assert_eq!(kv.stats(), MemKvStats::default());
            kv.set("ab", b"xyz".to_vec()).await.unwrap();
            kv.hset("hh", vec![("f".into(), b"1234".to_vec()), ("gg".into(), b"5".to_vec())])
                .await
                .unwrap();
            kv.get("ab").await.unwrap();
            kv.hget("hh", "f").await.unwrap();
            kv.hgetall("hh").await.unwrap();
            let stats = kv.stats();
            assert_eq!((stats.reads, stats.writes, stats.keys), (3, 2, 2));
            // "ab" + "xyz", then "hh" + "f" + "1234" + "gg" + "5".
            assert_eq!(stats.bytes, 5 + 10);
            assert_eq!(kv.keys(), ["ab", "hh"]);
        });
    }
}
