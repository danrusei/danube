use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{errors::Result, topic_storage::Segment};

mod in_memory;
pub use in_memory::InMemoryStorage;
mod disk;
pub use disk::DiskStorage;
mod aws_s3;
pub use aws_s3::S3Storage;

#[async_trait]
pub(crate) trait StorageBackend: Send + Sync + std::fmt::Debug + 'static {
    async fn get_segment(&self, id: usize) -> Result<Option<Arc<RwLock<Segment>>>>;
    async fn put_segment(&self, id: usize, segment: Arc<RwLock<Segment>>) -> Result<()>;
    async fn remove_segment(&self, id: usize) -> Result<()>;
    async fn contains_segment(&self, id: usize) -> Result<bool>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageBackendType {
    InMemory,
    Disk(String), // Path for local disk storage
    S3(String),   // S3 bucket name
}

impl StorageBackendType {
    pub(crate) fn create_backend(&self) -> Arc<dyn StorageBackend> {
        match self {
            StorageBackendType::InMemory => Arc::new(InMemoryStorage::new()),
            StorageBackendType::Disk(path) => Arc::new(DiskStorage::new(path.clone())),
            StorageBackendType::S3(bucket) => Arc::new(S3Storage::new(bucket.clone())),
        }
    }
}
