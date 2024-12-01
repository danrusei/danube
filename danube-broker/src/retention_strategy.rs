use anyhow::Result;

use crate::{consumer::MessageToSend, topic_storage::TopicStore};

#[derive(Debug)]
pub(crate) enum RetentionStrategy {
    // Does not store messages, sends them directly to the dispatcher
    NonReliable,
    // Stores messages in a queue for reliable delivery
    // TODO! - ensure that the messages are delivered in order and are acknowledged before removal from the queue
    // TODO! - TTL - implement a retention policy to remove messages from the queue after a certain period of time (e.g. 1 hour)
    Reliable(TopicStore),
}

impl RetentionStrategy {
    pub(crate) async fn store_message(&mut self, message: MessageToSend) -> Result<()> {
        match self {
            RetentionStrategy::Reliable(store) => {
                store.add_message(message);
            }
            RetentionStrategy::NonReliable => {}
        }
        Ok(())
    }
}

// impl RetentionStrategy {
//     // Segment acknowledgment from subscription
//     pub async fn acknowledge_segment(&mut self, segment_id: u64) -> Result<()> {
//         match self {
//             RetentionStrategy::Reliable(store) => {
//                 store.mark_segment_completed(segment_id).await?;
//                 Ok(())
//             }
//             RetentionStrategy::NonReliable => Ok(()),
//         }
//     }

//     // TTL cleanup for completed segments
//     pub async fn cleanup_expired_segments(&mut self, ttl: Duration) -> Result<()> {
//         match self {
//             RetentionStrategy::Reliable(store) => {
//                 let now = SystemTime::now();
//                 let completed_segments = store.get_completed_segments().await?;

//                 for segment in completed_segments {
//                     if now.duration_since(segment.completion_timestamp)? > ttl {
//                         store.remove_segment(segment.id).await?;
//                     }
//                 }
//                 Ok(())
//             }
//             RetentionStrategy::NonReliable => Ok(()),
//         }
//     }

//     // Segment-based message handling
//     pub async fn handle_message_segment(&mut self, segment: MessageSegment) -> Result<()> {
//         match self {
//             RetentionStrategy::NonReliable => self.dispatch_segment(segment).await,
//             RetentionStrategy::Reliable(store) => {
//                 let segment_id = store.store_segment(segment.clone()).await?;
//                 let result = self.dispatch_segment(segment).await;

//                 if result.is_err() {
//                     info!("Segment {} dispatch failed, retained for retry", segment_id);
//                 }
//                 Ok(())
//             }
//         }
//     }
// }
