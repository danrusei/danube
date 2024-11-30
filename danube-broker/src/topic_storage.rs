use dashmap::DashMap;
use std::sync::{Arc, RwLock};

use crate::consumer::MessageToSend;

// TopicStore is used only for reliable messaging
// It stores the segments in memory until are acknowledged by every subscription
#[derive(Debug)]
pub struct TopicStore {
    // List of segments
    segments: Vec<Arc<RwLock<Segment>>>,
    // Maximum messages per segment
    segment_capacity: usize,
    // Map of subscription name to last segment acknowledged
    subscriptions: DashMap<String, usize>,
    // ID for the current writable segment
    current_segment_id: usize,
}

impl TopicStore {
    pub fn new(segment_capacity: usize) -> Self {
        Self {
            segments: Vec::new(),
            segment_capacity,
            subscriptions: DashMap::new(),
            current_segment_id: 0,
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
}

#[derive(Debug, Clone)]
pub struct Segment {
    // Unique segment ID
    pub id: usize,
    // Messages in the segment
    pub messages: Vec<MessageToSend>,
}

impl Segment {
    pub fn new(id: usize, capacity: usize) -> Self {
        Self {
            id,
            messages: Vec::with_capacity(capacity),
        }
    }

    pub fn is_full(&self, capacity: usize) -> bool {
        self.messages.len() >= capacity
    }
}
