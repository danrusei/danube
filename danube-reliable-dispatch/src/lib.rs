mod topic_storage;
use topic_storage::TopicStore;
mod errors;
use errors::{ReliableDispatchError, Result};
mod dispatch;
pub use dispatch::SubscriptionDispatch;

use danube_client::StreamMessage;
use dashmap::DashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct ReliableDispatch {
    // Topic store is used to store messages in a queue for reliable delivery
    pub(crate) topic_store: TopicStore,
    //TODO here to stop the dispatcher ?
    //pub(crate) subscription_dispatch: HashMap<String, String>,
    // Map of subscription name to last acknowledged segment id
    pub(crate) subscriptions: Arc<DashMap<String, Arc<RwLock<usize>>>>,
    // Channel to send shutdown signal to the lifecycle management task
    shutdown_tx: tokio::sync::mpsc::Sender<()>,
}

impl ReliableDispatch {
    pub fn new(segment_size: usize, segment_ttl: u64) -> Self {
        let subscriptions: Arc<DashMap<String, Arc<RwLock<usize>>>> = Arc::new(DashMap::new());
        let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel(1);
        let subscriptions_cloned = Arc::clone(&subscriptions);

        let topic_store = TopicStore::new(segment_size, segment_ttl);
        // Start the lifecycle management task
        topic_store.start_lifecycle_management_task(shutdown_rx, subscriptions_cloned);

        Self {
            topic_store,
            subscriptions,
            shutdown_tx,
        }
    }

    pub async fn new_subscription_dispatch(
        &self,
        subscription_name: &str,
    ) -> Result<SubscriptionDispatch> {
        let sub_last_acked_segment = self
            .get_last_acknowledged_segment(subscription_name)
            .await?;

        let subscription_dispatch =
            SubscriptionDispatch::new(self.topic_store.clone(), sub_last_acked_segment);

        //self.subscription_dispatch.insert(subscription_name.to_string(), subscription_name.to_string());

        Ok(subscription_dispatch)
    }

    pub async fn store_message(&self, message: StreamMessage) -> Result<()> {
        self.topic_store.store_message(message);
        Ok(())
    }

    pub async fn add_subscription(&self, subscription_name: &str) -> Result<()> {
        self.subscriptions
            .insert(subscription_name.to_string(), Arc::new(RwLock::new(0)));
        Ok(())
    }

    pub async fn get_last_acknowledged_segment(
        &self,
        subscription_name: &str,
    ) -> Result<Arc<RwLock<usize>>> {
        match self.subscriptions.get(subscription_name) {
            Some(subscription) => Ok(Arc::clone(subscription.value())),
            None => Err(ReliableDispatchError::SubscriptionError(
                "Subscription not found".to_string(),
            )),
        }
    }
}

impl Drop for ReliableDispatch {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.try_send(());
    }
}
