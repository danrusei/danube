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
    async fn get_segment(&self, id: usize) -> Result<Option<Arc<RwLock<Segment>>>> {
        Ok(self.segments.get(&id).map(|segment| segment.clone()))
    }

    async fn put_segment(&self, id: usize, segment: Arc<RwLock<Segment>>) -> Result<()> {
        self.segments.insert(id, segment);
        Ok(())
    }

    async fn remove_segment(&self, id: usize) -> Result<()> {
        self.segments.remove(&id);
        Ok(())
    }

    async fn contains_segment(&self, id: usize) -> Result<bool> {
        Ok(self.segments.contains_key(&id))
    }
}
