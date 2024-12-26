use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    errors::{ReliableDispatchError, Result},
    storage_backend::StorageBackend,
    topic_storage::Segment,
};

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
        match self.segments.get(&id) {
            Some(segment) => Ok(Some(segment.clone())),
            None => Err(ReliableDispatchError::SegmentError(
                "Unable to find the segment".to_string(),
            )),
        }
    }

    async fn put_segment(&self, id: usize, segment: Arc<RwLock<Segment>>) -> Result<()> {
        match self.segments.insert(id, segment) {
            Some(_) => Ok(()),
            None => Err(ReliableDispatchError::SegmentError(
                "Unable to create the segment".to_string(),
            )),
        }
    }

    async fn remove_segment(&self, id: usize) -> Result<()> {
        match self.segments.remove(&id) {
            Some(_) => Ok(()),
            None => Err(ReliableDispatchError::SegmentError(
                "Unable to delete the segment".to_string(),
            )),
        }
    }

    async fn contains_segment(&self, id: usize) -> Result<bool> {
        match self.segments.contains_key(&id) {
            true => Ok(true),
            false => Ok(false),
        }
    }
}
