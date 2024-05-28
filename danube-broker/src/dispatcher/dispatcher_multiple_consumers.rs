use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{consumer::Consumer, subscription::Subscription, topic::Topic};

use crate::proto::consumer_request::SubscriptionType;

#[derive(Debug)]
pub(crate) struct DispatcherMultipleConsumers {
    topic_name: String,
    subscription_name: String,
    consumers: Vec<Arc<Mutex<Consumer>>>,
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
    pub(crate) async fn add_consumer(&mut self, consumer: Arc<Mutex<Consumer>>) -> Result<()> {
        // checks if adding a new consumer would exceed the maximum allowed consumers for the subscription
        self.consumers.push(consumer);
        todo!()
    }

    // manage the removal of consumers from the dispatcher
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

        todo!()
    }

    pub(crate) async fn get_consumers(&self) -> Option<&Vec<Arc<Mutex<Consumer>>>> {
        Some(&self.consumers)
    }

    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        self.consumers.iter().map(|consumer| async {
            consumer.lock().await.disconnect();
        });
        Ok(())
    }

    pub(crate) fn get_next_consumer(&self) -> Result<Arc<Mutex<Consumer>>> {
        if self.consumers.is_empty() {
            return Err(anyhow!("There are no consumers left"));
        }

        // find a way to sort the self.consumers based on priority or anything else
        if let Some(consumer) = self.consumers.get(0) {
            return Ok(consumer.clone());
        }

        Err(anyhow!("Unable to find the next consumer"))
    }

    pub(crate) async fn send_messages(&self, messages: Vec<u8>) -> Result<()> {
        // maybe wrap Vec<8> into a generic Message
        // selects the next available consumer based on available permits
        let consumer = self.get_next_consumer()?;
        let batch_size = 3; // to be calculated
        consumer
            .lock()
            .await
            .send_messages(messages, batch_size)
            .await?;
        todo!()
    }
}
