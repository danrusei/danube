mod errors;
pub(crate) use errors::Result;

mod store;

mod watch;

use store::MetadataStore;
pub use watch::{WatchEvent, WatchStream};

mod providers;
pub(crate) use providers::{etcd::EtcdStore, redis::RedisStore};

use async_trait::async_trait;

#[derive(Debug)]
pub enum StorageBackend {
    Etcd(EtcdStore),
    Redis(RedisStore),
    // Future backends: Consul, Zookeeper, PostgreSQL, MongoDB etc.
}

#[async_trait]
impl MetadataStore for StorageBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self {
            StorageBackend::Etcd(store) => store.get(key).await,
            StorageBackend::Redis(store) => store.get(key).await,
        }
    }

    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        match self {
            StorageBackend::Etcd(store) => store.put(key, value).await,
            StorageBackend::Redis(store) => store.put(key, value).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self {
            StorageBackend::Etcd(store) => store.delete(key).await,
            StorageBackend::Redis(store) => store.delete(key).await,
        }
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        match self {
            StorageBackend::Etcd(store) => store.watch(prefix).await,
            StorageBackend::Redis(store) => store.watch(prefix).await,
        }
    }
}
