use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::{consumer::Consumer, subscription::Subscription, topic::Topic};

use crate::proto::consumer_request::SubscriptionType;

#[derive(Debug)]
pub(crate) struct DispatcherSingleConsumer {
    topic_name: String,
    subscription_name: String,
    subscription_type: i32, // should be SubscriptionType,
    consumers: Vec<Consumer>,
    active_consumer: Option<Consumer>,
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
    pub(crate) fn pick_active_consumer(&mut self) -> bool {
        // sort the self.consumers ,after a specific logic, maybe highest priority

        if let Some(consumer) = self.consumers.pop() {
            self.active_consumer = Some(consumer);
            return true;
        }

        false
    }

    // sending messages to an active consumer
    pub(crate) async fn send_messages(&self, messages: Vec<Bytes>) -> Result<()> {
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

        current_consumer.send_messages(messages, 1).await?;

        Ok(())
    }

    // manage the addition of consumers to the dispatcher
    pub(crate) fn add_consumer(&mut self, consumer: Consumer) -> Result<()> {
        //Todo! alitle bit odd, have to recheck this function

        // Handle Exclusive Subscription ... should be SubscriptionType::Exclusive
        if consumer.subscription_type == 0 && !self.consumers.is_empty() {
            // connect to active consumer self.active_consumer
            return Ok(());
        }
        // Handle Failover  Subscription ... should be SubscriptionType::Failover
        if consumer.subscription_type == 2 {
            self.consumers.push(consumer.clone());
        }

        if !self.pick_active_consumer() {
            return Err(anyhow!(
                "couldn't make an active Consumer as there are none Consumers"
            ));
        }

        self.consumers.push(consumer);

        Ok(())
    }

    // manage the removal of consumers from the dispatcher
    pub(crate) fn remove_consumer(&mut self, consumer: Consumer) -> Result<()> {
        if let Some(pos) = self.consumers.iter().position(|x| *x == consumer) {
            self.consumers.remove(pos);
        }

        if self.consumers.is_empty() {
            self.active_consumer = None;
        }

        let _ = self.pick_active_consumer();

        Ok(())
    }

    pub(crate) fn get_consumers(&self) -> Option<&Vec<Consumer>> {
        todo!()
    }
}
