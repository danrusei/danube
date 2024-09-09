pub(crate) mod memory_segment_storage;

use dashmap::DashMap;

use crate::consumer::MessageToSend;

// Enum to differentiate between segment statuses (InProgress or Completed)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentStatus {
    InProgress,
    Completed,
}

pub(crate) struct SegmentStore {
    // Map of segment ID to its messages
    messages_per_segment: DashMap<u64, Vec<MessageToSend>>,

    // Map of topic ID to the list of segment IDs (segment ordering)
    segments_per_topic: DashMap<u64, Vec<u64>>, // topic_id -> list of segment_ids

    // Map of segment ID to its status (InProgress or Completed)
    segment_status: DashMap<u64, SegmentStatus>,
}

impl SegmentStore {
    pub(crate) fn new() -> Self {
        SegmentStore {
            messages_per_segment: DashMap::new(),
            segments_per_topic: DashMap::new(),
            segment_status: DashMap::new(),
        }
    }

    // Add a new message to an in-progress segment for a given topic
    pub(crate) fn add_message(&self, topic_id: u64, segment_id: u64, message: MessageToSend) {
        if let Some(status) = self.segment_status.get(&segment_id) {
            if *status == SegmentStatus::InProgress {
                let messages = self
                    .messages_per_segment
                    .entry(segment_id)
                    .or_insert_with(Vec::new);
                messages.push(message);
            }
        } else {
            // Initialize the segment if it doesn't exist
            self.segment_status
                .insert(segment_id, SegmentStatus::InProgress);
            self.segments_per_topic
                .entry(topic_id)
                .or_insert_with(Vec::new)
                .push(segment_id);
            self.messages_per_segment
                .entry(segment_id)
                .or_insert_with(Vec::new)
                .push(message);
        }
    }

    // Mark a segment as completed (from producer perspective)
    pub(crate) fn complete_segment(&self, segment_id: u64) {
        self.segment_status
            .insert(segment_id, SegmentStatus::Completed);
    }

    // Retrieve all messages from a segment (if it's completed)
    pub(crate) fn get_messages_from_segment(&self, segment_id: u64) -> Option<Vec<u64>> {
        if let Some(status) = self.segment_status.get(&segment_id) {
            if *status == SegmentStatus::Completed {
                return self
                    .messages_per_segment
                    .get(&segment_id)
                    .map(|messages| messages.iter().map(|m| m.sequence_id).collect());
            }
        }
        None
    }

    // Remove a completed segment
    pub fn remove_segment(&self, segment_id: u64) {
        self.messages_per_segment.remove(&segment_id);
        self.segment_status.remove(&segment_id);
    }

    // Get all segments for a topic
    pub fn get_segments_for_topic(&self, topic_id: u64) -> Option<Vec<u64>> {
        self.segments_per_topic.get(&topic_id).cloned()
    }

    // Check if a segment is completed
    pub fn is_segment_completed(&self, segment_id: u64) -> bool {
        self.segment_status
            .get(&segment_id)
            .map_or(false, |status| *status == SegmentStatus::Completed)
    }
}
