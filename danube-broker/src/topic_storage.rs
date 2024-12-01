use dashmap::DashMap;
use std::sync::{Arc, RwLock};

use crate::consumer::MessageToSend;

/// Segment is a collection of messages, the segment is closed for writing when it's capacity is reached
/// The segment is closed for reading when all subscriptions have acknowledged the segment
/// The segment is immutable after it's closed for writing
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
#[derive(Debug)]
pub(crate) struct TopicStore {
    // List of segments
    segments: Vec<Arc<RwLock<Segment>>>,
    // Maximum messages per segment
    segment_capacity: usize,
    // Time to live for segments in seconds
    segment_ttl: u64,
    // Map of subscription name to last acknowledged segment id
    subscriptions: Arc<DashMap<String, usize>>,
    // ID of the current writable segment
    current_segment_id: usize,
    // Channel to send shutdown signal to the lifecycle management task
    shutdown_tx: Option<tokio::sync::mpsc::Sender<()>>,
}

impl TopicStore {
    pub fn new(segment_capacity: usize, segment_ttl: u64) -> Self {
        Self {
            segments: Vec::new(),
            segment_capacity,
            segment_ttl,
            subscriptions: Arc::new(DashMap::new()),
            current_segment_id: 0,
            shutdown_tx: None,
        }
    }

    /// Add a new message to the topic
    pub fn add_message(&mut self, message: MessageToSend) {
        if self.segments.is_empty()
            || self
                .segments
                .last()
                .unwrap()
                .read()
                .unwrap()
                .is_full(self.segment_capacity)
        {
            self.create_new_segment();
        }

        let current_segment = self.segments.last().unwrap();
        let mut writable_segment = current_segment.write().unwrap();
        writable_segment.messages.push(message);
    }

    /// Create a new segment
    fn create_new_segment(&mut self) {
        let new_segment = Arc::new(RwLock::new(Segment::new(
            self.current_segment_id,
            self.segment_capacity,
        )));
        self.segments.push(new_segment);
        self.current_segment_id += 1;
    }

    /// Add a subscription to the topic
    pub fn add_subscription(&mut self, subscription_id: String) {
        self.subscriptions.entry(subscription_id).or_insert(0);
    }

    /// Get the next unacknowledged segment for a subscription
    pub fn get_next_segment(&self, subscription_id: &str) -> Option<Arc<RwLock<Segment>>> {
        let subscription_last_ack = self.subscriptions.get(subscription_id)?;
        let next_segment_id = *subscription_last_ack + 1;

        self.segments
            .iter()
            .find(|segment| segment.read().unwrap().id == next_segment_id)
            .cloned()
    }

    /// Acknowledge a segment for a subscription
    pub fn acknowledge_segment(&self, subscription_id: &str, segment_id: usize) {
        if let Some(mut last_ack) = self.subscriptions.get_mut(subscription_id) {
            if segment_id > *last_ack {
                *last_ack = segment_id;
            }
        }
    }

    /// Clean up segments acknowledged by all subscriptions
    pub fn cleanup_segments(&mut self) {
        let min_acknowledged = self
            .subscriptions
            .iter()
            .map(|entry| *entry.value())
            .min()
            .unwrap_or(0);

        self.segments
            .retain(|segment| segment.read().unwrap().id > min_acknowledged);
    }

    // Start the TopicStore lifecycle management task that have the following responsibilities:
    // - Clean up acknowledged segments
    // - Remove closed segments that are older than the TTL
    pub(crate) fn start_lifecycle_management(&mut self) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        // Clone necessary fields
        let mut segments = self.segments.clone(); // Clone the vector of Arc-wrapped segments
        let subscriptions = Arc::clone(&self.subscriptions); // Clone the Arc-wrapped DashMap
        let segment_ttl = self.segment_ttl;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Cleanup acknowledged segments
                        {
                            let mut to_remove = Vec::new();

                            for (index, segment) in segments.iter().enumerate() {
                                let segment = segment.read().unwrap();

                                // Check if all subscriptions acknowledged this segment
                                let acknowledged_by_all = subscriptions.iter().all(|entry| {
                                    let (_, last_acknowledged_id) = entry.pair();
                                    *last_acknowledged_id >= segment.id
                                });

                                if acknowledged_by_all {
                                    to_remove.push(index);
                                }
                            }

                            // Remove acknowledged segments
                            for index in to_remove.into_iter().rev() {
                                segments.remove(index);
                            }
                        }

                        // Remove segments older than TTL
                        {
                            let current_time = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs();

                            let mut to_remove = Vec::new();

                            for (index, segment) in segments.iter().enumerate() {
                                let segment = segment.read().unwrap();

                                if segment.close_time > 0 && (current_time - segment.close_time) >= segment_ttl {
                                    to_remove.push(index);
                                }
                            }

                            // Remove old segments
                            for index in to_remove.into_iter().rev() {
                                segments.remove(index);
                            }
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

impl Drop for TopicStore {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.try_send(());
        }
    }
}
