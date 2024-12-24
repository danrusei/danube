use danube_client::StreamMessage;
use dashmap::DashMap;
use std::sync::{Arc, RwLock};
use tracing::info;

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
    segments: Arc<DashMap<usize, Arc<RwLock<Segment>>>>,
    // Index of segments in the segments map
    segments_index: Arc<RwLock<Vec<usize>>>,
    // Maximum size per segment in bytes
    segment_size: usize,
    // Time to live for segments in seconds
    segment_ttl: u64,
    // ID of the current writable segment
    current_segment_id: Arc<RwLock<usize>>,
}

impl TopicStore {
    pub(crate) fn new(segment_size: usize, segment_ttl: u64) -> Self {
        // Convert segment size to bytes
        let segment_size_bytes = segment_size * 1024 * 1024;
        Self {
            segments: Arc::new(DashMap::new()),
            segments_index: Arc::new(RwLock::new(Vec::new())),
            segment_size: segment_size_bytes,
            segment_ttl,
            current_segment_id: Arc::new(RwLock::new(0)),
        }
    }

    /// Add a new message to the topic
    pub(crate) fn store_message(&self, message: StreamMessage) {
        let mut current_segment_id = self.current_segment_id.write().unwrap();
        let segment_id = *current_segment_id;

        // Get the current segment or create a new one if it doesn't exist
        let segment = self
            .segments
            .entry(segment_id)
            .or_insert_with(|| {
                // Create a new segment and update the index
                let new_segment =
                    Arc::new(RwLock::new(Segment::new(segment_id, self.segment_size)));
                let mut index = self.segments_index.write().unwrap();
                index.push(segment_id);
                new_segment
            })
            .clone();

        // Get the writable segment or create a new one if it doesn't exist
        let mut writable_segment = segment.write().unwrap();
        if writable_segment.is_full(self.segment_size) {
            // Mark current segment as closed for writing
            writable_segment.close_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Create a new segment
            *current_segment_id += 1;
            let new_segment_id = *current_segment_id;
            let new_segment =
                Arc::new(RwLock::new(Segment::new(new_segment_id, self.segment_size)));

            self.segments.insert(new_segment_id, new_segment.clone());

            // Update the index with the new segment ID
            let mut index = self.segments_index.write().unwrap();
            index.push(new_segment_id);

            // Add the message to the new segment
            let mut new_writable_segment = new_segment.write().unwrap();
            new_writable_segment.add_message(message);
            dbg!(
                "Store message in new segment {}",
                new_writable_segment.messages.len()
            );
        } else {
            // Add the message to the current writable segment
            writable_segment.add_message(message);
            dbg!(
                "Store message in current segment {}",
                writable_segment.messages.len()
            );
        }
    }

    // Get the next segment in the list based on the given segment ID
    // If the current segment is 0, it will return the first segment in the list
    pub(crate) fn get_next_segment(
        &self,
        current_segment_id: Option<usize>,
    ) -> Option<Arc<RwLock<Segment>>> {
        let index = self.segments_index.read().unwrap();

        match current_segment_id {
            None => {
                // Get the first segment if index is not empty
                if !index.is_empty() {
                    let first_segment_id = index[0];
                    return self
                        .segments
                        .get(&first_segment_id)
                        .map(|entry| entry.clone());
                }
                None
            }
            Some(segment_id) => {
                // Find the next segment after current_segment_id
                if let Some(pos) = index.iter().position(|&id| id == segment_id) {
                    if pos + 1 < index.len() {
                        let next_segment_id = index[pos + 1];
                        return self
                            .segments
                            .get(&next_segment_id)
                            .map(|entry| entry.clone());
                    }
                }
                None
            }
        }
    }

    pub(crate) fn contains_segment(&self, segment_id: usize) -> bool {
        self.segments.contains_key(&segment_id)
    }

    // Start the TopicStore lifecycle management task that have the following responsibilities:
    // - Clean up acknowledged segments
    // - Remove closed segments that are older than the TTL
    pub(crate) fn start_lifecycle_management_task(
        &self,
        mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
        subscriptions: Arc<DashMap<String, Arc<RwLock<usize>>>>,
    ) {
        let segments = self.segments.clone();
        let segments_index = self.segments_index.clone();
        let segment_ttl = self.segment_ttl;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::cleanup_acknowledged_segments(&segments, &segments_index, &subscriptions);
                        Self::cleanup_expired_segments(&segments, &segments_index, segment_ttl);
                    }
                    _ = shutdown_rx.recv() => break
                }
            }
        });
    }
    fn cleanup_acknowledged_segments(
        segments: &Arc<DashMap<usize, Arc<RwLock<Segment>>>>,
        segments_index: &Arc<RwLock<Vec<usize>>>,
        subscriptions: &Arc<DashMap<String, Arc<RwLock<usize>>>>,
    ) {
        let min_acknowledged_id = subscriptions
            .iter()
            .map(|entry| *entry.value().read().unwrap())
            .min()
            .unwrap_or(0);

        segments.retain(|id, segment| {
            let segment_read = segment.read().unwrap();
            // Keep segments that are not closed or have an ID greater than the minimum acknowledged ID
            let should_keep = segment_read.close_time == 0 || *id > min_acknowledged_id;
            if !should_keep {
                info!(
                    "Dropping segment {} from TopicStore - acknowledged by all subscriptions",
                    id
                );
            }
            should_keep
        });

        let mut index = segments_index.write().unwrap();
        index.retain(|&id| id > min_acknowledged_id);
    }

    fn cleanup_expired_segments(
        segments: &Arc<DashMap<usize, Arc<RwLock<Segment>>>>,
        segments_index: &Arc<RwLock<Vec<usize>>>,
        segment_ttl: u64,
    ) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        segments.retain(|id, segment| {
            let segment = segment.read().unwrap();
            let keep = segment.close_time == 0 || (current_time - segment.close_time) < segment_ttl;
            if !keep {
                let mut index = segments_index.write().unwrap();
                index.retain(|&index_id| index_id != *id);
                info!("Dropping segment {} from TopicStore - TTL expired", id);
            }
            keep
        });
    }
}
