use dashmap::DashMap;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct SegmentStore {
    // Map segment ID to segment data
    segments: DashMap<u64, Vec<u8>>,
    // Map topic names to list of segment_ids
    segments_per_topic: DashMap<String, Vec<u64>>,
    // Map segment ID to segment metadata
    segments_metadata: DashMap<u64, SegmentMetadata>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct SegmentMetadata {
    // segment id
    segment_id: u64,
    // segment belong to this topic
    topic: String,
    // message sequence id to message metadata
    messages_metadata: DashMap<u64, MessageMetadata>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct MessageMetadata {
    // the sequence of the message
    sequence_id: u64,
    // the segment where the message is stored
    segment_id: u64,
    // Starting byte offset of the message in segment data
    offset: usize,
    // Length of the message in bytes
    length: usize,
}

impl SegmentStore {
    #[allow(dead_code)]
    pub fn new() -> Self {
        SegmentStore {
            segments: DashMap::new(),
            segments_per_topic: DashMap::new(),
            segments_metadata: DashMap::new(),
        }
    }

    // StoreSegment adds segment data and metadata to the store
    #[allow(dead_code)]
    pub fn store_segment(&mut self, segment_metadata: SegmentMetadata, data: Vec<u8>) {
        let topic = segment_metadata.topic.clone();
        let segment_id = segment_metadata.segment_id;

        self.segments.insert(segment_id, data);
        self.segments_metadata.insert(segment_id, segment_metadata);

        // Update segments per topic if topic exists
        self.segments_per_topic
            .entry(topic.clone())
            .or_insert(Vec::new())
            .push(segment_id);
    }

    // GetSegment retrieves segment data and metadata by segment id
    #[allow(dead_code)]
    pub fn get_segment(&self, segment_id: u64) -> Option<(Vec<u8>, SegmentMetadata)> {
        let segment_data = self.segments.get(&segment_id)?;
        let segment_metadata = self.segments_metadata.get(&segment_id)?;
        Some((segment_data.clone(), segment_metadata.clone()))
    }

    // RemoveSegment removes segment data and metadata based on segment id
    #[allow(dead_code)]
    pub fn remove_segment(&mut self, segment_id: u64) -> Option<SegmentMetadata> {
        let _ = self.segments.remove(&segment_id)?;
        let metadata = self.segments_metadata.remove(&segment_id)?;

        // Update segments per topic if it existed
        let topic = &metadata.1.topic;
        self.segments_per_topic
            .get_mut(topic)
            .and_then(|mut segment_ids| {
                Some(segment_ids.remove((segment_id - 1).try_into().ok()?))
            });

        Some(metadata.1)
    }

    // GetMessage retrieves a specific message based on segment id and message sequence id
    #[allow(dead_code)]
    pub fn get_message(&self, segment_id: u64, message_sequence_id: u64) -> Option<Vec<u8>> {
        self.segments_metadata
            .get(&segment_id)
            .and_then(|metadata| {
                metadata
                    .messages_metadata
                    .get(&message_sequence_id)
                    .map(|message_metadata| {
                        let segment_data = self.segments.get(&segment_id).unwrap();
                        let start = message_metadata.offset;
                        let end = message_metadata.offset + message_metadata.length;
                        segment_data[start..end].to_vec()
                    })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_segment() {
        let mut store = SegmentStore::new();
        let metadata = SegmentMetadata {
            segment_id: 1,
            topic: "test_topic".to_string(),
            messages_metadata: DashMap::new(),
        };
        let data = vec![1, 2, 3, 4];

        store.store_segment(metadata.clone(), data.clone());

        assert_eq!(store.segments.get(&1).unwrap().clone(), data);
        assert_eq!(
            store.segments_metadata.get(&1).unwrap().topic.clone(),
            metadata.topic
        );
    }

    #[test]
    fn test_get_segment() {
        let mut store = SegmentStore::new();
        let metadata = SegmentMetadata {
            segment_id: 1,
            topic: "test_topic".to_string(),
            messages_metadata: DashMap::new(),
        };
        let data = vec![1, 2, 3, 4];

        store.store_segment(metadata.clone(), data.clone());

        let (segment_data, segment_metadata) = store.get_segment(1).unwrap();

        assert_eq!(segment_data, data);
        assert_eq!(segment_metadata.topic, metadata.topic);
    }

    #[test]
    fn test_get_segment_not_found() {
        let store = SegmentStore::new();
        assert!(store.get_segment(10).is_none());
    }

    #[test]
    fn test_remove_segment() {
        let mut store = SegmentStore::new();
        let metadata = SegmentMetadata {
            segment_id: 1,
            topic: "test_topic".to_string(),
            messages_metadata: DashMap::new(),
        };
        let message_data = vec![1, 2, 3, 4];
        let _message_metadata = MessageMetadata {
            sequence_id: 1,
            segment_id: 1,
            offset: 0,
            length: message_data.len(),
        };

        store.store_segment(metadata.clone(), message_data.clone());

        let removed_metadata = store.remove_segment(1).unwrap();

        assert!(removed_metadata.messages_metadata.is_empty());
        assert!(store.get_segment(1).is_none());
    }

    #[test]
    fn test_remove_segment_not_found() {
        let mut store = SegmentStore::new();
        assert!(store.remove_segment(10).is_none());
    }

    #[test]
    fn test_get_message() {
        let mut store = SegmentStore::new();
        let message_data = vec![5, 6, 7, 8];
        let segment_metadata = SegmentMetadata {
            segment_id: 1,
            topic: "test_topic".to_string(),
            messages_metadata: DashMap::new(),
        };
        let message_metadata = MessageMetadata {
            sequence_id: 1,
            segment_id: 1,
            offset: 5,
            length: message_data.len(),
        };
        segment_metadata
            .messages_metadata
            .insert(message_metadata.sequence_id, message_metadata.clone());

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8];
        store.store_segment(segment_metadata, data);

        let retrieved_message = store.get_message(1, 1).unwrap();

        assert_eq!(retrieved_message, message_data);
    }

    #[test]
    fn test_get_message_not_found() {
        let store = SegmentStore::new();
        assert_eq!(store.get_message(1, 1), None);
    }
}
