use anyhow::{anyhow, Result};
use danube_client::StreamMessage;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

use crate::proto::TopicDeliveryStrategy;
use crate::topic_storage::TopicStore;

#[derive(Debug)]
pub(crate) enum DeliveryStrategy {
    // Does not store messages, sends them directly to the dispatcher
    NonReliable,
    // Stores messages in a queue for reliable delivery
    // TODO! - ensure that the messages are delivered in order and are acknowledged before removal from the queue
    // TODO! - TTL - implement a retention policy to remove messages from the queue after a certain period of time (e.g. 1 hour)
    Reliable(PersistentStorage),
}

#[derive(Debug)]
pub(crate) struct PersistentStorage {
    // Topic store is used to store messages in a queue for reliable delivery
    pub(crate) topic_store: TopicStore,
    // Map of subscription name to last acknowledged segment id
    pub(crate) subscriptions: Arc<DashMap<String, Arc<RwLock<usize>>>>,
    // Channel to send shutdown signal to the lifecycle management task
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
}

impl PersistentStorage {
    pub(crate) fn new(segment_size: usize, segment_ttl: u64) -> Self {
        let topic_store = TopicStore::new(segment_size, segment_ttl);
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

    pub(crate) async fn store_message(&self, message: StreamMessage) -> Result<()> {
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

impl Drop for PersistentStorage {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.try_send(());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ConfigDeliveryStrategy {
    pub(crate) strategy: String,
    pub(crate) retention_period: u64,
    pub(crate) segment_size: usize,
}

impl Default for ConfigDeliveryStrategy {
    fn default() -> Self {
        ConfigDeliveryStrategy {
            strategy: "non_reliable".to_string(),
            retention_period: 3600,
            segment_size: 50,
        }
    }
}

// Implement conversions from ProtoTypeSchema to SchemaType
impl From<TopicDeliveryStrategy> for ConfigDeliveryStrategy {
    fn from(strategy: TopicDeliveryStrategy) -> Self {
        ConfigDeliveryStrategy {
            strategy: strategy.strategy,
            retention_period: strategy.retention_period,
            segment_size: strategy.segment_size as usize,
        }
    }
}
