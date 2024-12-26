use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{errors::Result, storage_backend::StorageBackend, topic_storage::Segment};

#[derive(Debug)]
pub struct InMemoryStorage {
    segments: DashMap<usize, Arc<RwLock<Segment>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            segments: DashMap::new(),
        }
    }
}

#[async_trait]
impl StorageBackend for InMemoryStorage {
    async fn get_segment(&self, _id: usize) -> Result<Option<Arc<RwLock<Segment>>>> {
        todo!()
    }
    async fn put_segment(&self, _id: usize, _segment: Arc<RwLock<Segment>>) -> Result<()> {
        todo!()
    }
    async fn remove_segment(&self, _id: usize) -> Result<()> {
        todo!()
    }
    async fn contains_segment(&self, _id: usize) -> Result<bool> {
        todo!()
    }
}
