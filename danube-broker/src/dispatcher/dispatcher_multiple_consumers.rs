use anyhow::{anyhow, Result};
use bytes::Bytes;

use crate::{consumer::Consumer, subscription::Subscription, topic::Topic};

use crate::proto::consumer_request::SubscriptionType;

#[derive(Debug)]
pub(crate) struct DispatcherMultipleConsumers {
    topic_name: String,
    subscription_name: String,
    consumers: Vec<Consumer>,
}

impl DispatcherMultipleConsumers {
    pub(crate) fn new(topic_name: &str, subscription_name: &str) -> Self {
        DispatcherMultipleConsumers {
            topic_name: topic_name.into(),
            subscription_name: subscription_name.into(),
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

    pub(crate) fn get_consumers(&self) -> Option<&Vec<Consumer>> {
        Some(&self.consumers)
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
