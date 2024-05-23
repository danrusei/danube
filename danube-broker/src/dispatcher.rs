use anyhow::{anyhow, Ok, Result};
use bytes::Bytes;

use crate::{
    consumer::Consumer,
    subscription::{Subscription, SubscriptionType},
    topic::Topic,
};

#[derive(Debug)]
pub(crate) struct DispatcherSingleConsumer {
    topic: Topic,
    consumers: Vec<Consumer>,
    active_consumer: Option<Consumer>,
}

impl DispatcherSingleConsumer {
    pub(crate) fn new(
        subscription_type: SubscriptionType,
        topic: Topic,
        subsc: Subscription,
    ) -> Self {
        DispatcherSingleConsumer {
            topic,
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
    pub(crate) async fn send_messages(messages: Vec<Bytes>) -> Result<()> {
        // maybe wrap Vec<Bytes> into a generic Message

        todo!()
    }

    // manage the addition of consumers to the dispatcher
    pub(crate) fn add_consumer(
        &mut self,
        consumer: Consumer,
        subscription_type: SubscriptionType,
    ) -> Result<()> {
        // Handle Exclusive Subscription
        if subscription_type == SubscriptionType::Exclusive && !self.consumers.is_empty() {
            // connect to active consumer self.active_consumer
            return Ok(());
        }
        if subscription_type == SubscriptionType::Failover {
            self.consumers.push(consumer);
        }

        if self.pick_active_consumer().is_ok_and(|x| x == false) {
            return Err(anyhow!("couldn't find an active consumer"));
        }

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

#[derive(Debug)]
pub(crate) struct DispatcherMultipleConsumers {
    topic: Topic,
    consumers: Vec<Consumer>,
}

impl DispatcherMultipleConsumers {
    pub(crate) fn new(topic: Topic) -> Self {
        DispatcherMultipleConsumers {
            topic,
            consumers: Vec::new(),
        }
    }

    // manage the addition of consumers to the dispatcher
    pub(crate) fn add_consumer(&mut self, consumer: Consumer) -> Result<()> {
        // checks if adding a new consumer would exceed the maximum allowed consumers for the subscription
        self.consumers.push(consumer);
        todo!()
    }

    // manage the removal of consumers from the dispatcher
    pub(crate) fn remove_consumer(&mut self, consumer: Consumer) -> Result<()> {
        if let Some(pos) = self.consumers.iter().position(|x| *x == consumer) {
            self.consumers.remove(pos);
        }
        todo!()
    }

    pub(crate) fn get_consumers(&self) -> Result<&Vec<Consumer>> {
        Ok(&self.consumers)
    }

    pub(crate) fn disconnect_all_consumers(&self) -> Result<()> {
        self.consumers.iter().map(|consumer| consumer.disconnect());
        Ok(())
    }

    pub(crate) fn get_next_consumer(&self) -> Result<Consumer> {
        if self.consumers.is_empty() {
            return Err(anyhow!("There are no consumers left"));
        }

        // find a way to sort the self.consumers based on priority or anything else
        if let Some(consumer) = self.consumers.get(0) {
            return Ok(consumer.clone());
        }

        Err(anyhow!("Unable to find the next consumer"))
    }

    pub(crate) async fn send_messages(&self, messages: Vec<Bytes>) -> Result<()> {
        // maybe wrap Vec<8> into a generic Message
        // selects the next available consumer based on available permits
        let consumer = self.get_next_consumer()?;
        let batch_size = 3; // to be calculated
        consumer.send_messages(messages, batch_size).await?;
        todo!()
    }
}
