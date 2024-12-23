mod topic_storage;
use topic_storage::TopicStore;
mod errors;
use errors::Result;
mod reliable_dispatch;
use reliable_dispatch::ConsumerDispatch;

use danube_client::StreamMessage;
use dashmap::DashMap;
use std::sync::{Arc, RwLock};

pub struct ReliableDelivery {
    // Topic store is used to store messages in a queue for reliable delivery
    pub topic_store: TopicStore,
    // subscriptions: SubscriptionManager,
    // Map of subscription name to last acknowledged segment id
    pub subscriptions: Arc<DashMap<String, Arc<RwLock<usize>>>>,
    // Channel to send shutdown signal to the lifecycle management task
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
}

impl ReliableDelivery {
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

impl Drop for ReliableDelivery {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.try_send(());
    }
}
