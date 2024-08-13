use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{trace, warn};

use crate::consumer::{Consumer, MessageToSend};

#[derive(Debug)]
pub(crate) struct DispatcherSingleConsumer {
    consumers: Vec<Arc<Mutex<Consumer>>>,
    active_consumer: RwLock<Option<Arc<Mutex<Consumer>>>>,
}

impl DispatcherSingleConsumer {
    pub(crate) fn new() -> Self {
        DispatcherSingleConsumer {
            consumers: Vec::new(),
            active_consumer: RwLock::new(None),
        }
    }

    // Pick an active consumer for a topic for subscription.
    pub(crate) async fn pick_active_consumer(&self) -> bool {
        // sort the self.consumers ,after a specific logic, maybe highest priority

        let mut candidate = None;

        for consumer in &self.consumers {
            let guard = consumer.lock().await;
            // validates that the consumer has called the receive methods which populates the Consumer tx field
            if guard.tx.is_some() && guard.status {
                candidate = Some(consumer.clone());
                break;
            }
        }

        if let Some(consumer) = candidate {
            let mut active_consumer = self.active_consumer.write().await;
            *active_consumer = Some(consumer);
            true
        } else {
            false
        }
    }

    // sending messages to an active consumer
    pub(crate) async fn send_messages(&self, messages: MessageToSend) -> Result<()> {
        // Try to acquire the read lock on the active consumer
        let active_consumer = {
            let guard = self.active_consumer.read().await;
            match &*guard {
                Some(consumer) => consumer.clone(),
                None => return Err(anyhow::anyhow!("There is no active Consumer")),
            }
        };

        //Todo!
        // 1. check first if the Consumer allow to send the messages
        // 2. filter the messages for consumers
        // 3. other permits like dispatch rate limiter, quota etc

        let mut consumer_guard = active_consumer.lock().await;

        // check if the consumer is active
        if !consumer_guard.status {
            drop(consumer_guard);

            // Pick a new active consumer
            if !self.pick_active_consumer().await {
                return Err(anyhow!(
                    "There is no active consumer to dispatch the message"
                ));
            }
        } else {
            consumer_guard.send_messages(messages, 1).await?;
        }

        Ok(())
    }

    // manage the addition of consumers to the dispatcher
    pub(crate) async fn add_consumer(&mut self, consumer: Arc<Mutex<Consumer>>) -> Result<()> {
        // Handle Exclusive Subscription
        // The consumer addition is not allowed if there are consumers in the list and Subscription is Exclusive
        let consumer_subscription_type;

        {
            let consumer_guard = consumer.lock().await;
            consumer_subscription_type = consumer_guard.subscription_type;

            // if the subscription is Shared should not be routed to this dispatcher
            if consumer_subscription_type == 1 {
                return Err(anyhow!("Erroneous routing, Shared subscription should use dispatcher multiple consumer"));
            }

            if consumer_subscription_type == 0 && !self.consumers.is_empty() {
                // connect to active consumer self.active_consumer
                warn!("Not allowed to add the Consumer: {}, the Exclusive subscription can't be shared with other consumers", consumer_guard.consumer_id);
                return Err(anyhow!("Not allowed to add the Consumer, the Exclusive subscription can't be shared with other consumers"));
            }
        }

        if self.consumers.is_empty() {
            self.active_consumer = Some(consumer.clone()).into()
        } else {
            if !self.pick_active_consumer().await {
                return Err(anyhow!("Unable to pick an active Consumer"));
            }
        }

        // add Exclusive and Failover consumer to dispatcher
        self.consumers.push(consumer.clone());

        let consumer_name;

        {
            let consumer_guard = consumer.lock().await;
            consumer_name = consumer_guard.consumer_name.clone();
        }

        trace!(
            "The dispatcher DispatcherSingleConsumer has added the consumer {}",
            consumer_name
        );

        Ok(())
    }

    // manage the removal of consumers from the dispatcher
    #[allow(dead_code)]
    pub(crate) async fn remove_consumer(&mut self, consumer: Consumer) -> Result<()> {
        // Find the position asynchronously
        let pos = {
            let mut pos = None;
            for (index, x) in self.consumers.iter().enumerate() {
                if x.lock().await.consumer_id == consumer.consumer_id {
                    pos = Some(index);
                    break;
                }
            }
            pos
        };

        // If a position was found, remove the consumer at that position
        if let Some(pos) = pos {
            self.consumers.remove(pos);
        }

        // Check if the consumers list is empty and update the active consumer
        if self.consumers.is_empty() {
            self.active_consumer = None.into();
        }

        let _ = self.pick_active_consumer();

        Ok(())
    }

    pub(crate) async fn disconnect_all_consumers(&self) -> Result<Vec<u64>> {
        let mut consumers = Vec::new();

        for consumer in self.consumers.iter() {
            let consumer_id = consumer.lock().await.disconnect();
            consumers.push(consumer_id)
        }
        Ok(consumers)
    }

    pub(crate) fn get_consumers(&self) -> &Vec<Arc<Mutex<Consumer>>> {
        &self.consumers
    }
}
