use async_trait::async_trait;

use crate::{errors::Result, watch::WatchStream};

#[async_trait]
pub trait MetadataStore: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn watch(&self, prefix: &str) -> Result<WatchStream>;
}
