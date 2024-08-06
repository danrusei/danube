use anyhow::{anyhow, Result};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::trace;

use crate::consumer::{Consumer, MessageToSend};

#[derive(Debug)]
pub(crate) struct DispatcherMultipleConsumers {
    consumers: Vec<Arc<Mutex<Consumer>>>,
    index_consumer: AtomicUsize,
}

impl DispatcherMultipleConsumers {
    pub(crate) fn new() -> Self {
        DispatcherMultipleConsumers {
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
    pub(crate) async fn remove_consumer(&mut self, consumer_id: u64) -> Result<()> {
        // Find the position asynchronously
        let pos = {
            let mut pos = None;
            for (index, x) in self.consumers.iter().enumerate() {
                let consumer = x.lock().await;
                if consumer.consumer_id == consumer_id {
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

    pub(crate) async fn disconnect_all_consumers(&self) -> Result<Vec<u64>> {
        let mut consumers = Vec::new();

        for consumer in self.consumers.iter() {
            let consumer_id = consumer.lock().await.disconnect();
            consumers.push(consumer_id)
        }
        Ok(consumers)
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

    pub(crate) async fn send_messages(&self, messages: MessageToSend) -> Result<()> {
        // selects the next available consumer based on available permits
        let consumer = self.get_next_consumer()?;
        let mut consumer_guard = consumer.lock().await;
        let batch_size = 1; // to be calculated

        // check if the consumer is active, if not remove from the dispatcher
        if !consumer_guard.status {
            // can't be removed for now, as it force alot of functions to be moved to mutable
            // maybe use an backgroud process that remove unused resources
            // like disconnected consumers and producers
            // or maybe move to Arc<Mutex<Vec<Consumer>>> ??
            //return self.remove_consumer(consumer_guard.consumer_id).await;

            return Ok(());
        }

        consumer_guard.send_messages(messages, batch_size).await?;
        trace!(
            "Dispatcher is sending the message to consumer: {}",
            consumer_guard.consumer_id
        );
        Ok(())
    }
}
