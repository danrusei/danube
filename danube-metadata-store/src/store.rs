use async_trait::async_trait;
use etcd_client::{GetOptions, PutOptions};
use serde_json::Value;

use crate::{errors::Result, watch::WatchStream};

#[derive(Debug)]
pub enum MetaOptions {
    None,
    EtcdGet(GetOptions),
    EtcdPut(PutOptions),
}

#[async_trait]
pub trait MetadataStore: Send + Sync + 'static {
    async fn get(&self, key: &str, get_options: MetaOptions) -> Result<Option<Value>>;
    async fn get_childrens(&self, path: &str) -> Result<Vec<String>>;
    async fn put(&self, key: &str, value: Value, put_options: MetaOptions) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn watch(&self, prefix: &str) -> Result<WatchStream>;
}
