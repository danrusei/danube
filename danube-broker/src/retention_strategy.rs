use anyhow::{anyhow, Result};
use dashmap::DashMap;
use std::sync::{Arc, RwLock};

use crate::{consumer::MessageToSend, topic_storage::TopicStore};

#[derive(Debug)]
pub(crate) enum RetentionStrategy {
    // Does not store messages, sends them directly to the dispatcher
    NonReliable,
    // Stores messages in a queue for reliable delivery
    // TODO! - ensure that the messages are delivered in order and are acknowledged before removal from the queue
    // TODO! - TTL - implement a retention policy to remove messages from the queue after a certain period of time (e.g. 1 hour)
    Reliable(ReliableStorage),
}

#[derive(Debug)]
pub(crate) struct ReliableStorage {
    // Topic store is used to store messages in a queue for reliable delivery
    pub(crate) topic_store: TopicStore,
    // Map of subscription name to last acknowledged segment id
    pub(crate) subscriptions: Arc<DashMap<String, Arc<RwLock<usize>>>>,
    // Channel to send shutdown signal to the lifecycle management task
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
}

impl ReliableStorage {
    pub(crate) fn new(segment_capacity: usize, segment_ttl: u64) -> Self {
        let topic_store = TopicStore::new(segment_capacity, segment_ttl);
        let subscriptions: Arc<DashMap<String, Arc<RwLock<usize>>>> = Arc::new(DashMap::new());
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let subscriptions_cloned = Arc::clone(&subscriptions);
        // Start the lifecycle management task
        topic_store.start_lifecycle_management_task(shutdown_rx, subscriptions_cloned);

        Self {
            topic_store,
            subscriptions,
            shutdown_tx,
        }
    }

    pub(crate) async fn store_message(&self, message: MessageToSend) -> Result<()> {
        self.topic_store.store_message(message);
        Ok(())
    }

    pub(crate) async fn add_subscription(&self, subscription_name: &str) -> Result<()> {
        self.subscriptions
            .insert(subscription_name.to_string(), Arc::new(RwLock::new(0)));
        Ok(())
    }

    pub(crate) async fn get_last_acknowledged_segment(
        &self,
        subscription_name: &str,
    ) -> Result<Arc<RwLock<usize>>> {
        match self.subscriptions.get(subscription_name) {
            Some(subscription) => Ok(Arc::clone(subscription.value())),
            None => Err(anyhow!("Subscription not found")),
        }
    }
}

impl Drop for ReliableStorage {
    fn drop(&mut self) {
        self.shutdown_tx.try_send(());
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
