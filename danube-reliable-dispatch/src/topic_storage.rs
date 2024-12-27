use danube_client::StreamMessage;
use dashmap::DashMap;
use std::sync::{atomic::AtomicUsize, Arc};
use tokio::sync::RwLock;
use tracing::trace;

use crate::{errors::Result, storage_backend::StorageBackend};

/// Segment is a collection of messages, the segment is closed for writing when it's capacity is reached
/// The segment is closed for reading when all subscriptions have acknowledged the segment
/// The segment is immutable after it's closed for writing
/// The messages in the segment are in the order of arrival
#[derive(Debug, Clone)]
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
    pub(crate) segment_ttl: u64,
    // ID of the current writable segment
    pub(crate) current_segment_id: Arc<RwLock<usize>>,
}

impl TopicStore {
    pub(crate) fn new(
        storage: Arc<dyn StorageBackend>,
        segment_size: usize,
        segment_ttl: u64,
    ) -> Self {
        // Convert segment size to bytes
        let segment_size_bytes = segment_size * 1024 * 1024;
        Self {
            storage,
            segments_index: Arc::new(RwLock::new(Vec::new())),
            segment_size: segment_size_bytes,
            segment_ttl,
            current_segment_id: Arc::new(RwLock::new(0)),
        }
    }

    /// Add a new message to the topic, if the segment is full, a new segment is created
    pub(crate) async fn store_message(&self, message: StreamMessage) -> Result<()> {
        let mut current_segment_id = self.current_segment_id.write().await;
        let segment_id = *current_segment_id;

        let segment = match self.storage.get_segment(segment_id).await? {
            Some(segment) => segment,
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

        let close_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

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
            let mut index = self.segments_index.write().await;
            if let Some(entry) = index.iter_mut().find(|(id, _)| *id == segment_id) {
                entry.1 = close_time;
            }

            *current_segment_id += 1;
            let new_segment_id = *current_segment_id;
            let new_segment =
                Arc::new(RwLock::new(Segment::new(new_segment_id, self.segment_size)));

            self.storage
                .put_segment(new_segment_id, new_segment.clone())
                .await?;
            index.push((new_segment_id, 0));

            let mut new_writable_segment = new_segment.write().await;
            new_writable_segment.add_message(message);
        }

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
        self.storage.contains_segment(segment_id).await
    }

    // Start the TopicStore lifecycle management task that have the following responsibilities:
    // - Clean up acknowledged segments
    // - Remove closed segments that are older than the TTL
    pub(crate) fn start_lifecycle_management_task(
        &self,
        mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
        subscriptions: Arc<DashMap<String, Arc<AtomicUsize>>>,
    ) {
        let storage = self.storage.clone();
        let segments_index = self.segments_index.clone();
        let segment_ttl = self.segment_ttl;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::cleanup_acknowledged_segments(&storage, &segments_index, &subscriptions).await;
                        Self::cleanup_expired_segments(&storage, &segments_index, segment_ttl).await;
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
        segment_ttl: u64,
    ) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut index = segments_index.write().await;

        let expired_segments: Vec<usize> = index
            .iter()
            .filter(|(_, close_time)| {
                *close_time > 0 && (current_time - *close_time) >= segment_ttl
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
