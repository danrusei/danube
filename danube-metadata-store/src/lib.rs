mod errors;
pub use errors::MetadataError;
pub(crate) use errors::Result;

mod store;
pub use store::MetaOptions;
pub use store::MetadataStore;

mod watch;
pub use watch::{WatchEvent, WatchStream};

mod providers;
pub use providers::{
    etcd::{EtcdStore, KeyValueVersion},
    in_memory::MemoryStore,
    redis::RedisStore,
};

use async_trait::async_trait;
pub use etcd_client::GetOptions as EtcdGetOptions;
use etcd_client::LeaseGrantResponse;
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum StorageBackend {
    Etcd(EtcdStore),
    Redis(RedisStore),
    InMemory(MemoryStore), // InMemory is used for testing purposes
                           // Future backends: Consul, Zookeeper, PostgreSQL, MongoDB etc.
}

#[async_trait]
impl MetadataStore for StorageBackend {
    async fn get(&self, key: &str, get_options: MetaOptions) -> Result<Option<Value>> {
        match self {
            StorageBackend::Etcd(store) => store.get(key, get_options).await,
            StorageBackend::Redis(store) => store.get(key, get_options).await,
            StorageBackend::InMemory(store) => store.get(key, get_options).await,
        }
    }

    async fn get_childrens(&self, path: &str) -> Result<Vec<String>> {
        match self {
            StorageBackend::Etcd(store) => store.get_childrens(path).await,
            StorageBackend::Redis(store) => store.get_childrens(path).await,
            StorageBackend::InMemory(store) => store.get_childrens(path).await,
        }
    }

    async fn put(&self, key: &str, value: Value, put_options: MetaOptions) -> Result<()> {
        match self {
            StorageBackend::Etcd(store) => store.put(key, value, put_options).await,
            StorageBackend::Redis(store) => store.put(key, value, put_options).await,
            StorageBackend::InMemory(store) => store.put(key, value, put_options).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self {
            StorageBackend::Etcd(store) => store.delete(key).await,
            StorageBackend::Redis(store) => store.delete(key).await,
            StorageBackend::InMemory(store) => store.delete(key).await,
        }
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        match self {
            StorageBackend::Etcd(store) => store.watch(prefix).await,
            StorageBackend::Redis(store) => store.watch(prefix).await,
            StorageBackend::InMemory(store) => store.watch(prefix).await,
        }
    }
}

impl StorageBackend {
    pub async fn create_lease(&self, ttl: i64) -> Result<LeaseGrantResponse> {
        match self {
            StorageBackend::Etcd(store) => store.create_lease(ttl).await,
            _ => Err(MetadataError::UnsupportedOperation.into()),
        }
    }

    pub async fn keep_lease_alive(&self, lease_id: i64, role: &str) -> Result<()> {
        match self {
            StorageBackend::Etcd(store) => store.keep_lease_alive(lease_id, role).await,
            _ => Err(MetadataError::UnsupportedOperation.into()),
        }
    }

    pub async fn put_with_lease(&self, key: &str, value: Value, lease_id: i64) -> Result<()> {
        match self {
            StorageBackend::Etcd(store) => store.put_with_lease(key, value, lease_id).await,
            _ => Err(MetadataError::UnsupportedOperation.into()),
        }
    }
    pub async fn get_bulk(&self, prefix: &str) -> Result<Vec<KeyValueVersion>> {
        match self {
            StorageBackend::Etcd(store) => store.get_bulk(prefix).await,
            _ => Err(MetadataError::UnsupportedOperation.into()),
        }
    }
}
