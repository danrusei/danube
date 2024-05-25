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
    pub(crate) fn pick_active_consumer(&mut self) -> Result<bool> {
        // sort the self.consumers ,after a specific logic, maybe highest priority

        if let Some(consumer) = self.consumers.get(0) {
            self.active_consumer = Some(consumer.clone());
        }

        Ok(true)
    }

    // sending messages to an active consumer
    pub(crate) async fn send_messages(&self, messages: Vec<Bytes>) -> Result<()> {
        // maybe wrap Vec<Bytes> into a generic Message

        todo!()
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

        if self.pick_active_consumer().is_ok_and(|x| x == false) {
            return Err(anyhow!("couldn't find an active consumer"));
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
}
