mod etcd_metadata_store;
mod etcd_watch;
mod memory_metadata_store;
use anyhow::Result;
pub(crate) use etcd_metadata_store::EtcdMetadataStore;
pub(crate) use etcd_watch::{etcd_watch_prefixes, ETCDWatchEvent};
pub(crate) use memory_metadata_store::MemoryMetadataStore;

use etcd_client::{GetOptions, PutOptions};
use serde_json::Value;

// MetadataStore is the storage layer for Danube Metadata
#[allow(dead_code)]
pub(crate) trait MetadataStore {
    // Read the value of one key, identified by the path
    async fn get(&mut self, path: &str, get_options: MetaOptions) -> Result<Option<Value>>;
    // Return all the paths that are children to the specific path.
    async fn get_childrens(&mut self, path: &str) -> Result<Vec<String>>;
    // Put a new value for a given key
    async fn put(&mut self, path: &str, value: Value, put_options: MetaOptions) -> Result<()>;
    // Delete the key / value from the store
    async fn delete(&mut self, path: &str) -> Result<Option<Value>>;
    // Delete a key-value pair and all the children nodes.
    async fn delete_recursive(&mut self, path: &str) -> Result<()>;
    // The client is used for specific operations, which are not put or get
    fn get_client(&mut self) -> Option<etcd_client::Client>;
    // Register a listener that will be called on changes in the underlying store.
    //fn register_listener(listener: String) //this should be migrated to Consumer<Notification>
}

// TODO! this is not optimal if you need GetOptions::new().with_prefix()

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum MetadataStorage {
    MemoryStore(MemoryMetadataStore),
    EtcdStore(EtcdMetadataStore),
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum MetaOptions {
    None,
    EtcdGet(GetOptions),
    EtcdPut(PutOptions),
}

impl MetadataStore for MetadataStorage {
    async fn get(&mut self, path: &str, get_options: MetaOptions) -> Result<Option<Value>> {
        match self {
            MetadataStorage::MemoryStore(store) => store.get(path, MetaOptions::None).await,
            MetadataStorage::EtcdStore(store) => store.get(path, get_options).await,
        }
    }
    async fn get_childrens(&mut self, path: &str) -> Result<Vec<String>> {
        match self {
            MetadataStorage::MemoryStore(store) => store.get_childrens(path).await,
            MetadataStorage::EtcdStore(store) => store.get_childrens(path).await,
        }
    }

    async fn put(&mut self, path: &str, value: Value, put_options: MetaOptions) -> Result<()> {
        match self {
            MetadataStorage::MemoryStore(store) => store.put(path, value, MetaOptions::None).await,
            MetadataStorage::EtcdStore(store) => store.put(path, value, put_options).await,
        }
    }

    async fn delete(&mut self, path: &str) -> Result<Option<Value>> {
        match self {
            MetadataStorage::MemoryStore(store) => store.delete(path).await,
            MetadataStorage::EtcdStore(store) => store.delete(path).await,
        }
    }

    async fn delete_recursive(&mut self, path: &str) -> Result<()> {
        match self {
            MetadataStorage::MemoryStore(store) => store.delete_recursive(path).await,
            MetadataStorage::EtcdStore(store) => store.delete_recursive(path).await,
        }
    }
    fn get_client(&mut self) -> Option<etcd_client::Client> {
        match self {
            MetadataStorage::MemoryStore(_store) => None,
            MetadataStorage::EtcdStore(store) => store.get_client(),
        }
    }
}

#[allow(dead_code)]
pub(crate) struct MetadataStoreConfig {
    meta_store: String,
    config_file: Option<String>,
}

impl MetadataStoreConfig {
    pub(crate) fn new() -> Self {
        MetadataStoreConfig {
            meta_store: "metadata-store".to_owned(),
            config_file: None,
        }
    }
}
