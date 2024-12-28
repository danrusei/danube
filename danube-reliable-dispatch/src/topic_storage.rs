use danube_client::{ReliableOptions, RetentionPolicy, StreamMessage};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::{atomic::AtomicUsize, Arc};
use tokio::sync::{Mutex, RwLock};
use tracing::trace;

use crate::{errors::Result, storage_backend::StorageBackend};

/// Segment is a collection of messages, the segment is closed for writing when it's capacity is reached
/// The segment is closed for reading when all subscriptions have acknowledged the segment
/// The segment is immutable after it's closed for writing
/// The messages in the segment are in the order of arrival
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Segment {
    // Unique segment ID
    pub(crate) id: usize,
    // Segment close time, is the time when the segment is closed for writing
    pub(crate) close_time: u64,
    // Messages in the segment
    pub(crate) messages: Vec<StreamMessage>,
    // Current size of the segment in bytes
    pub(crate) current_size: usize,
}

impl Segment {
    pub fn new(id: usize, capacity: usize) -> Self {
        Self {
            id,
            close_time: 0,
            messages: Vec::with_capacity(capacity),
            current_size: 0,
        }
    }

    pub fn is_full(&self, max_size: usize) -> bool {
        self.current_size >= max_size
    }

    pub fn add_message(&mut self, message: StreamMessage) {
        self.current_size += message.size();
        self.messages.push(message);
    }
}

// TopicStore is used only for reliable messaging
// It stores the segments in memory until are acknowledged by every subscription
#[derive(Debug, Clone)]
pub(crate) struct TopicStore {
    // Concurrent map of segment ID to segments
    //pub(crate) segments: Arc<DashMap<usize, Arc<RwLock<Segment>>>>,
    // Storage backend for segments
    pub(crate) storage: Arc<dyn StorageBackend>,
    // Index of segments store (segment_id, close_time) pairs
    pub(crate) segments_index: Arc<RwLock<Vec<(usize, u64)>>>,
    // Maximum size per segment in bytes
    pub(crate) segment_size: usize,
    // Time to live for segments in seconds
    pub(crate) retention_period: u64,
    // ID of the current writable segment
    pub(crate) current_segment_id: Arc<RwLock<usize>>,
    // Cached segment, used to avoid expensive call to storage while storing a message
    cached_segment: Arc<Mutex<Option<Arc<RwLock<Segment>>>>>,
}

impl TopicStore {
    pub(crate) fn new(storage: Arc<dyn StorageBackend>, reliable_options: ReliableOptions) -> Self {
        // Convert segment size from MB to Bytes
        let segment_size_bytes = reliable_options.segment_size * 1024 * 1024;
        Self {
            storage,
            segments_index: Arc::new(RwLock::new(Vec::new())),
            segment_size: segment_size_bytes,
            retention_period: reliable_options.retention_period,
            current_segment_id: Arc::new(RwLock::new(0)),
            cached_segment: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) async fn store_message(&self, message: StreamMessage) -> Result<()> {
        let segment_id = *self.current_segment_id.write().await;
        let segment = self.get_or_create_segment(segment_id).await?;

        let close_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // check if segment is full, if so mark it as closed and create a new segment
        let should_create_new_segment = {
            let mut writable_segment = segment.write().await;
            if writable_segment.is_full(self.segment_size) {
                writable_segment.close_time = close_time;
                true
            } else {
                writable_segment.add_message(message.clone());
                false
            }
        };

        if should_create_new_segment {
            self.handle_segment_full(segment_id, close_time, message)
                .await?;
        }

        Ok(())
    }

    // Checks if the segment is present in the cache, if not it fetches it from the storage
    // if no segment is found in the storage, it creates a new segment
    async fn get_or_create_segment(&self, segment_id: usize) -> Result<Arc<RwLock<Segment>>> {
        let mut cached = self.cached_segment.lock().await;
        match &*cached {
            Some(seg) => Ok(seg.clone()),
            None => {
                let new_seg = match self.storage.get_segment(segment_id).await? {
                    Some(seg) => seg,
                    None => {
                        let new_segment =
                            Arc::new(RwLock::new(Segment::new(segment_id, self.segment_size)));
                        self.storage
                            .put_segment(segment_id, new_segment.clone())
                            .await?;
                        let mut index = self.segments_index.write().await;
                        index.push((segment_id, 0));
                        new_segment
                    }
                };
                *cached = Some(new_seg.clone());
                Ok(new_seg)
            }
        }
    }

    // if the segment is full, it will close the segment and create a new one with next ID
    async fn handle_segment_full(
        &self,
        segment_id: usize,
        close_time: u64,
        message: StreamMessage,
    ) -> Result<()> {
        let mut index = self.segments_index.write().await;
        if let Some(entry) = index.iter_mut().find(|(id, _)| *id == segment_id) {
            entry.1 = close_time;
        }

        let new_segment_id = segment_id + 1;
        let new_segment = Arc::new(RwLock::new(Segment::new(new_segment_id, self.segment_size)));

        self.storage
            .put_segment(new_segment_id, new_segment.clone())
            .await?;
        index.push((new_segment_id, 0));

        *self.cached_segment.lock().await = Some(new_segment.clone());
        *self.current_segment_id.write().await = new_segment_id;

        let mut new_writable_segment = new_segment.write().await;
        new_writable_segment.add_message(message);

        Ok(())
    }

    // Get the next segment in the list based on the given segment ID
    // If the current segment is 0, it will return the first segment in the list
    pub(crate) async fn get_next_segment(
        &self,
        current_segment_id: Option<usize>,
    ) -> Result<Option<Arc<RwLock<Segment>>>> {
        let index = self.segments_index.read().await;

        match current_segment_id {
            None => {
                if !index.is_empty() {
                    let first_segment_id = index[0].0;
                    return self.storage.get_segment(first_segment_id).await;
                }
                Ok(None)
            }
            Some(segment_id) => {
                if let Some(pos) = index.iter().position(|(id, _)| *id == segment_id) {
                    if pos + 1 < index.len() {
                        let next_segment_id = index[pos + 1].0;
                        return self.storage.get_segment(next_segment_id).await;
                    }
                }
                Ok(None)
            }
        }
    }

    pub(crate) async fn contains_segment(&self, segment_id: usize) -> Result<bool> {
        let index = self.segments_index.read().await;
        Ok(index.iter().any(|(id, _)| *id == segment_id))
    }

    // Start the TopicStore lifecycle management task that have the following responsibilities:
    // - Clean up acknowledged segments
    // - Remove closed segments that are older than the TTL
    pub(crate) fn start_lifecycle_management_task(
        &self,
        mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
        subscriptions: Arc<DashMap<String, Arc<AtomicUsize>>>,
        retention_policy: RetentionPolicy,
    ) {
        let storage = self.storage.clone();
        let segments_index = self.segments_index.clone();
        let retention_period = self.retention_period;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        match retention_policy {
                            RetentionPolicy::RetainUntilAck => {
                                Self::cleanup_acknowledged_segments(&storage, &segments_index, &subscriptions).await;
                            }
                            RetentionPolicy::RetainUntilExpire => {
                                Self::cleanup_expired_segments(&storage, &segments_index, retention_period).await;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => break
                }
            }
        });
    }

    pub(crate) async fn cleanup_acknowledged_segments(
        storage: &Arc<dyn StorageBackend>,
        segments_index: &Arc<RwLock<Vec<(usize, u64)>>>,
        subscriptions: &Arc<DashMap<String, Arc<AtomicUsize>>>,
    ) {
        let min_acknowledged_id = subscriptions
            .iter()
            .map(|entry| entry.value().load(std::sync::atomic::Ordering::Acquire))
            .min()
            .unwrap_or(0);

        let mut index = segments_index.write().await;
        let segments_to_remove: Vec<usize> = index
            .iter()
            .filter(|(id, close_time)| *close_time > 0 && *id <= min_acknowledged_id)
            .map(|(id, _)| *id)
            .collect();

        for segment_id in &segments_to_remove {
            if let Err(e) = storage.remove_segment(*segment_id).await {
                trace!("Failed to remove segment {}: {:?}", segment_id, e);
                continue;
            }
            trace!(
                "Dropped segment {} - acknowledged by all subscriptions",
                segment_id
            );
        }

        //index.retain(|(id, _)| *id >= min_acknowledged_id);
        index.retain(|(id, _)| !segments_to_remove.contains(id));
    }

    pub(crate) async fn cleanup_expired_segments(
        storage: &Arc<dyn StorageBackend>,
        segments_index: &Arc<RwLock<Vec<(usize, u64)>>>,
        retention_period: u64,
    ) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut index = segments_index.write().await;

        let expired_segments: Vec<usize> = index
            .iter()
            .filter(|(_, close_time)| {
                *close_time > 0 && (current_time - *close_time) >= retention_period
            })
            .map(|(id, _)| *id)
            .collect();

        for segment_id in &expired_segments {
            if let Err(e) = storage.remove_segment(*segment_id).await {
                trace!("Failed to remove expired segment {}: {:?}", segment_id, e);
                continue;
            }
            trace!("Dropped segment {} - TTL expired", segment_id);
        }

        index.retain(|(id, _)| !expired_segments.contains(id));
    }
}
