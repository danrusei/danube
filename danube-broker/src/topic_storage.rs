use dashmap::DashMap;
use std::sync::{Arc, RwLock};

use crate::consumer::MessageToSend;

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
    pub(crate) messages: Vec<MessageToSend>,
}

impl Segment {
    pub fn new(id: usize, capacity: usize) -> Self {
        Self {
            id,
            close_time: 0,
            messages: Vec::with_capacity(capacity),
        }
    }

    pub fn is_full(&self, capacity: usize) -> bool {
        self.messages.len() >= capacity
    }
}

// TopicStore is used only for reliable messaging
// It stores the segments in memory until are acknowledged by every subscription
#[derive(Debug, Clone)]
pub(crate) struct TopicStore {
    // Concurrent map of segment ID to segments
    segments: Arc<DashMap<usize, Arc<RwLock<Segment>>>>,
    // Maximum messages per segment
    segment_capacity: usize,
    // Time to live for segments in seconds
    segment_ttl: u64,
    // ID of the current writable segment
    current_segment_id: Arc<RwLock<usize>>,
}

impl TopicStore {
    pub fn new(segment_capacity: usize, segment_ttl: u64) -> Self {
        Self {
            segments: Arc::new(DashMap::new()),
            segment_capacity,
            segment_ttl,
            current_segment_id: Arc::new(RwLock::new(0)),
        }
    }

    /// Add a new message to the topic
    pub fn store_message(&self, message: MessageToSend) {
        let mut current_segment_id = self.current_segment_id.write().unwrap();
        let segment_id = *current_segment_id;

        // Get the current segment or create a new one if it doesn't exist
        let segment = self
            .segments
            .entry(segment_id)
            .or_insert_with(|| {
                Arc::new(RwLock::new(Segment::new(segment_id, self.segment_capacity)))
            })
            .clone();

        // Get the writable segment or create a new one if it doesn't exist
        let mut writable_segment = segment.write().unwrap();
        if writable_segment.is_full(self.segment_capacity) {
            *current_segment_id += 1;
            let new_segment = Arc::new(RwLock::new(Segment::new(
                *current_segment_id,
                self.segment_capacity,
            )));
            self.segments.insert(*current_segment_id, new_segment);
        } else {
            writable_segment.messages.push(message);
        }
    }

    // Get the segment by ID
    pub fn get_segment(&self, segment_id: usize) -> Option<Arc<RwLock<Segment>>> {
        self.segments.get(&segment_id).map(|entry| entry.clone())
    }

    // Start the TopicStore lifecycle management task that have the following responsibilities:
    // - Clean up acknowledged segments
    // - Remove closed segments that are older than the TTL
    pub(crate) fn start_lifecycle_management_task(
        &self,
        mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
        subscriptions: Arc<DashMap<String, Arc<RwLock<usize>>>>,
    ) {
        // Clone necessary fields
        let segments = self.segments.clone();
        let segment_ttl = self.segment_ttl;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Cleanup acknowledged segments
                        {
                            let min_acknowledged_id = subscriptions
                                .iter()
                                .map(|entry| *entry.value().read().unwrap())
                                .min()
                                .unwrap_or(0);

                            segments.retain(|id, _| *id > min_acknowledged_id);
                        }

                        // Remove segments older than TTL
                        {
                            let current_time = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs();

                            segments.retain(|_, segment| {
                                let segment = segment.read().unwrap();
                                segment.close_time == 0 || (current_time - segment.close_time) < segment_ttl
                            });
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        // Shutdown signal received
                        break;
                    }
                }
            }
        });
    }
}
