use anyhow::{anyhow, Result};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::trace;

use crate::consumer::Consumer;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct DispatcherMultipleConsumers {
    topic_name: String,
    subscription_name: String,
    consumers: Vec<Arc<Mutex<Consumer>>>,
    index_consumer: AtomicUsize,
}

impl DispatcherMultipleConsumers {
    pub(crate) fn new(topic_name: &str, subscription_name: &str) -> Self {
        DispatcherMultipleConsumers {
            topic_name: topic_name.into(),
            subscription_name: subscription_name.into(),
            consumers: Vec::new(),
            index_consumer: AtomicUsize::new(0),
        }
    }

    // manage the addition of consumers to the dispatcher
    pub(crate) async fn add_consumer(&mut self, consumer: Arc<Mutex<Consumer>>) -> Result<()> {
        // checks if adding a new consumer would exceed the maximum allowed consumers for the subscription
        self.consumers.push(consumer);

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

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn get_consumers(&self) -> Option<&Vec<Arc<Mutex<Consumer>>>> {
        Some(&self.consumers)
    }

    #[allow(dead_code)]
    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        let _ = self.consumers.iter().map(|consumer| async {
            let _ = consumer.lock().await.disconnect();
        });
        Ok(())
    }

    pub(crate) fn get_next_consumer(&self) -> Result<Arc<Mutex<Consumer>>> {
        if self.consumers.is_empty() {
            return Err(anyhow!("There are no consumers left"));
        }

        let index = self
            .index_consumer
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        if index == self.consumers.len() - 1 {
            let _ = self
                .index_consumer
                .swap(0, std::sync::atomic::Ordering::SeqCst);
        }

        // find a way to sort the self.consumers based on priority or anything else
        if let Some(consumer) = self.consumers.get(index) {
            return Ok(consumer.clone());
        }

        Err(anyhow!("Unable to find the next consumer"))
    }

    pub(crate) async fn send_messages(&self, messages: Vec<u8>) -> Result<()> {
        // maybe wrap Vec<8> into a generic Message
        // selects the next available consumer based on available permits
        let consumer = self.get_next_consumer()?;
        let consumer = consumer.lock().await;
        let batch_size = 1; // to be calculated
        consumer.send_messages(messages, batch_size).await?;
        trace!(
            "Dispatcher is sending the message to consumer: {}",
            consumer.consumer_id
        );
        Ok(())
    }
}
