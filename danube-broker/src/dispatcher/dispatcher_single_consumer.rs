use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::consumer::Consumer;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct DispatcherSingleConsumer {
    topic_name: String,
    subscription_name: String,
    subscription_type: i32, // should be SubscriptionType,
    consumers: Vec<Arc<Mutex<Consumer>>>,
    active_consumer: Option<Arc<Mutex<Consumer>>>,
}

impl DispatcherSingleConsumer {
    pub(crate) fn new(
        topic_name: impl Into<String>,
        subscription_name: impl Into<String>,
        subscription_type: i32, // should be SubscriptionType,
    ) -> Self {
        DispatcherSingleConsumer {
            topic_name: topic_name.into(),
            subscription_name: subscription_name.into(),
            subscription_type,
            active_consumer: None,
            consumers: Vec::new(),
        }
    }

    // Pick an active consumer for a topic for subscription.
    pub(crate) async fn pick_active_consumer(&mut self) -> bool {
        // sort the self.consumers ,after a specific logic, maybe highest priority

        let mut candidate = None;

        for consumer in &self.consumers {
            let guard = consumer.lock().await;
            // validates that the consumer has called the receive methods which populates the Consumer tx field
            if guard.tx.is_some() {
                candidate = Some(consumer.clone());
                break;
            }
        }

        if let Some(consumer) = candidate {
            self.active_consumer = Some(consumer);
            true
        } else {
            false
        }
    }

    // sending messages to an active consumer
    pub(crate) async fn send_messages(&self, messages: Vec<u8>) -> Result<()> {
        let current_consumer = if let Some(consumer) = &self.active_consumer {
            consumer
        } else {
            return Err(anyhow!("There is no active Consumer"));
        };

        //Todo!
        // 1. check first if the Consumer allow to send the messages
        // 2. filter the messages for consumers
        // 3. other permits like dispatch rate limiter, quota etc

        // maybe wrap Vec<Bytes> into a generic Message

        current_consumer
            .lock()
            .await
            .send_messages(messages, 1)
            .await?;

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
                return Err(anyhow!("Not allowed to add the Consumer, the Exclusive subscription can't be shared with other consumers"));
            }
        }

        if self.consumers.is_empty() {
            self.active_consumer = Some(consumer.clone())
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

        info!(
            "The dispatcher {:?} has added the consumer {}",
            self, consumer_name
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
            self.active_consumer = None;
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

    #[allow(dead_code)]
    pub(crate) async fn get_consumers(&self) -> Option<&Vec<Arc<Mutex<Consumer>>>> {
        todo!()
    }
}
