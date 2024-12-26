#[cfg(test)]
use crate::topic_storage::{Segment, TopicStore};
#[cfg(test)]
use danube_client::{MessageID, StreamMessage};
#[cfg(test)]
use dashmap::DashMap;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::{atomic::AtomicUsize, Arc};
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
fn create_test_message(payload: Vec<u8>) -> StreamMessage {
    StreamMessage {
        request_id: 1,
        msg_id: MessageID {
            sequence_id: 1,
            producer_id: 1,
            topic_name: "/default/test-topic".to_string(),
            broker_addr: "localhost:6650".to_string(),
        },
        payload,
        publish_time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        producer_name: "test-producer".to_string(),
        subscription_name: Some("test-subscription".to_string()),
        attributes: HashMap::new(),
    }
}

/// Tests basic segment initialization with correct default values
/// Validates:
/// - Segment ID assignment
/// - Initial close time is 0
/// - Empty message vector
/// - Zero initial size
#[test]
fn test_segment_creation() {
    let segment = Segment::new(1, 1024);
    assert_eq!(segment.id, 1);
    assert_eq!(segment.close_time, 0);
    assert_eq!(segment.current_size, 0);
    assert!(segment.messages.is_empty());
}

/// Tests adding messages to a segment
/// Validates:
/// - Message storage
/// - Size tracking
/// - Message count
#[test]
fn test_segment_message_handling() {
    let mut segment = Segment::new(1, 1024);
    let message = create_test_message(vec![1, 2, 3]);

    segment.add_message(message.clone());
    assert_eq!(segment.messages.len(), 1);
    assert_eq!(segment.current_size, message.size());
}

/// Tests segment size limit behavior
/// Validates:
/// - Size limit checks
/// - Segment full condition
/// - Multiple message additions
#[test]
fn test_segment_size_limit() {
    let mut segment = Segment::new(1, 1024);
    let message = create_test_message(vec![0; 512]);

    assert!(!segment.is_full(1024));
    segment.add_message(message.clone());
    assert!(!segment.is_full(1024));
    segment.add_message(message);
    assert!(segment.is_full(1024));
}

/// Tests basic message storage in TopicStore
/// Validates:
/// - Message storage functionality
/// - Initial segment creation
/// - Message retrieval
#[test]
fn test_topic_store_message_storage() {
    let store = TopicStore::new(1, 3600);
    let message = create_test_message(vec![1, 2, 3]);

    store.store_message(message.clone());
    let segment = store.get_next_segment(None).unwrap();
    let segment_read = segment.read().unwrap();
    assert_eq!(segment_read.messages.len(), 1);
}

/// Tests segment transition when size limit is reached
/// Validates:
/// - New segment creation on size limit
/// - Segment ID progression
/// - Message distribution across segments
#[test]
fn test_topic_store_segment_transition() {
    let store = TopicStore::new(1, 3600); // 1MB segment size
    let large_message = create_test_message(vec![0; 1024 * 1024]); // 1MB message

    store.store_message(large_message.clone());
    let message = create_test_message(vec![1]);
    store.store_message(message); // Should create new segment

    let first_segment = store.get_next_segment(None).unwrap();
    let second_segment = store
        .get_next_segment(Some(first_segment.read().unwrap().id))
        .unwrap();

    assert_ne!(
        first_segment.read().unwrap().id,
        second_segment.read().unwrap().id
    );
}

/// Tests segment cleanup based on TTL
/// Validates:
/// - Expired segment removal
/// - TTL enforcement
/// - Segment tracking after cleanup
#[test]
fn test_topic_store_cleanup() {
    let store = TopicStore::new(1, 1);
    let subscriptions = Arc::new(DashMap::new());
    let subscription_id = "test_sub".to_string();
    subscriptions.insert(subscription_id.clone(), Arc::new(AtomicUsize::new(0)));

    let message = create_test_message(vec![1, 2, 3]);
    store.store_message(message);

    // Force segment close time to be in the past
    let segment = store.get_next_segment(None).unwrap();
    {
        let mut segment_write = segment.write().unwrap();
        segment_write.close_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 2;
    }

    TopicStore::cleanup_expired_segments(&store.segments, &store.segments_index, 1);

    assert!(!store.contains_segment(0));
}

/// Tests segment cleanup based on acknowledgments
/// Validates:
/// - Acknowledged segment removal
/// - Subscription tracking
/// - Segment cleanup based on subscription state
#[test]
fn test_topic_store_acknowledged_cleanup() {
    let store = TopicStore::new(1, 3600);
    let subscriptions = Arc::new(DashMap::new());
    let subscription_id = "test_sub".to_string();
    subscriptions.insert(subscription_id.clone(), Arc::new(AtomicUsize::new(1)));

    let message = create_test_message(vec![1, 2, 3]);
    store.store_message(message);

    let segment = store.get_next_segment(None).unwrap();
    {
        let mut segment_write = segment.write().unwrap();
        segment_write.close_time = 1;
    }

    TopicStore::cleanup_acknowledged_segments(
        &store.segments,
        &store.segments_index,
        &subscriptions,
    );

    assert!(!store.contains_segment(0));
}
