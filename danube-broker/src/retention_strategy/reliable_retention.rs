use anyhow::Result;
use dashmap::DashMap;
use std::collections::HashSet;

use crate::retention_strategy::RetentionStrategy;
use crate::storage::SegmentStore;

// Reliable retention strategy
#[derive(Debug, Clone)]
pub struct ReliableRetention {
    // Map of subscription name to the list of segment IDs being consumed by that subscription
    segments_per_subscription: DashMap<String, Vec<u64>>,

    // Track which subscriptions have acknowledged which message IDs per segment
    ack_status_per_segment: DashMap<u64, DashMap<u64, HashSet<String>>>,

    // Track all subscriptions
    subscriptions: HashSet<String>,
}

impl ReliableRetention {
    pub fn new() -> Self {
        ReliableRetention {
            segments_per_subscription: DashMap::new(),
            ack_status_per_segment: DashMap::new(),
            subscriptions: HashSet::new(),
        }
    }

    // Add a new subscription
    pub fn add_subscription(&mut self, subscription_name: String) {
        self.subscriptions.insert(subscription_name.clone());
        self.segments_per_subscription
            .insert(subscription_name, Vec::new());
    }

    // Track a segment for consumption (even if it's still in-progress)
    pub fn track_segment(
        &self,
        subscription_name: &str,
        segment_id: u64,
        segment_store: &SegmentStore,
    ) {
        let ack_status = self
            .ack_status_per_segment
            .entry(segment_id)
            .or_insert_with(DashMap::new);

        // Initialize ack status for messages that have already arrived in this segment
        if let Some(messages) = segment_store.messages_per_segment.get(&segment_id) {
            for message in messages.iter() {
                ack_status
                    .entry(message.sequence_id)
                    .or_insert_with(HashSet::new);
            }
        }

        // Associate this segment with the subscription
        if let Some(mut segments) = self.segments_per_subscription.get_mut(subscription_name) {
            if !segments.contains(&segment_id) {
                segments.push(segment_id);
            }
        }
    }

    // Acknowledge a message for a subscription
    pub fn ack_message(
        &self,
        segment_id: u64,
        message_id: u64,
        subscription_name: &str,
        segment_store: &SegmentStore,
    ) {
        if let Some(ack_map) = self.ack_status_per_segment.get_mut(&segment_id) {
            if let Some(ack_set) = ack_map.get_mut(&message_id) {
                ack_set.insert(subscription_name.to_string());

                // Check if the message has been acknowledged by all subscriptions
                if ack_set.len() == self.subscriptions.len() {
                    ack_map.remove(&message_id);

                    // Check if the segment is fully acknowledged and completed
                    if ack_map.is_empty() && segment_store.is_segment_completed(segment_id) {
                        // All messages in this segment have been acknowledged
                        self.mark_segment_for_deletion(segment_id, segment_store);
                    }
                }
            }
        }
    }

    // Mark a segment for deletion once all messages in the segment have been acknowledged
    fn mark_segment_for_deletion(&self, segment_id: u64, segment_store: &SegmentStore) {
        // Remove the segment from SegmentStore
        segment_store.remove_segment(segment_id);

        // Remove the segment from all subscriptions
        for mut segments in self.segments_per_subscription.iter_mut() {
            segments.retain(|&seg_id| seg_id != segment_id);
        }

        // Remove the ack status for this segment
        self.ack_status_per_segment.remove(&segment_id);
    }

    // Check for new messages in an in-progress segment and update tracking
    pub fn update_in_progress_segment(&self, segment_id: u64, segment_store: &SegmentStore) {
        if let Some(messages) = segment_store.messages_per_segment.get(&segment_id) {
            let ack_status = self
                .ack_status_per_segment
                .entry(segment_id)
                .or_insert_with(DashMap::new);
            for message in messages.iter() {
                ack_status
                    .entry(message.sequence_id)
                    .or_insert_with(HashSet::new);
            }
        }
    }

    // Mark a segment as completed and check if it can be deleted
    pub fn complete_segment(&self, segment_id: u64, segment_store: &SegmentStore) {
        if segment_store.is_segment_completed(segment_id) {
            if let Some(ack_map) = self.ack_status_per_segment.get(&segment_id) {
                if ack_map.is_empty() {
                    // All messages have been acknowledged
                    self.mark_segment_for_deletion(segment_id, segment_store);
                }
            }
        }
    }
}
