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

        if let Some(consumer) = self.consumers.pop() {
            // validates that the consumer has called the receive methods
            // that's populate the Consumer tx field
            if consumer.lock().await.tx.is_some() {
                self.active_consumer = Some(consumer);
            }
            return true;
        }

        false
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
        // the consumer addition is not allowed if there are consumers in the list and Subscription is Exclusive
        let consumer_guard = consumer.lock().await;

        if consumer_guard.subscription_type == 0 && !self.consumers.is_empty() {
            // connect to active consumer self.active_consumer
            return Err(anyhow!("Not allowed to add the Consumer, the Exclusive subscription can't be shared with other consumers"));
        }

        // Handle Failover Subscription ... should be SubscriptionType::Failover
        if consumer_guard.subscription_type == 2 {
            self.consumers.push(consumer.clone());
            return Ok(());
        }

        // Handle Shared Subscription
        self.consumers.push(consumer.clone());

        if !self.pick_active_consumer().await {
            return Err(anyhow!("unable to pick an active Consumer"));
        }

        info!(
            "The dispatcher {:?} has added the consumer {}",
            self, consumer_guard.consumer_name
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

    #[allow(dead_code)]
    pub(crate) async fn get_consumers(&self) -> Option<&Vec<Arc<Mutex<Consumer>>>> {
        todo!()
    }
}
