use anyhow::{anyhow, Result};
use std::sync::atomic::AtomicUsize;
use tracing::trace;

use crate::{consumer::MessageToSend, subscription::ConsumerInfo};

#[derive(Debug)]
pub(crate) struct DispatcherMultipleConsumers {
    consumers: Vec<ConsumerInfo>,
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
    pub(crate) async fn add_consumer(&mut self, consumer: ConsumerInfo) -> Result<()> {
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
                if x.consumer_id == consumer_id {
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

    pub(crate) fn get_consumers(&self) -> &Vec<ConsumerInfo> {
        &self.consumers
    }

    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        Ok(())
    }

    pub(crate) async fn send_messages(&self, messages: MessageToSend) -> Result<()> {
        // Attempt to get an active consumer and send messages
        if let Ok(consumer) = self.find_next_active_consumer().await {
            //let batch_size = 1; // to be calculated
            consumer.tx_broker.send(messages).await?;
            trace!(
                "Dispatcher is sending the message to consumer: {}",
                consumer.consumer_id
            );
            Ok(())
        } else {
            Err(anyhow!("There are no active consumers on this dispatcher"))
        }
    }

    async fn find_next_active_consumer(&self) -> Result<ConsumerInfo> {
        let num_consumers = self.consumers.len();

        for _ in 0..num_consumers {
            let consumer = self.get_next_consumer()?;

            if !*consumer.status.lock().await {
                continue;
            }

            return Ok(consumer);
        }

        return Err(anyhow!("unable to find an active consumer"));
    }

    pub(crate) fn get_next_consumer(&self) -> Result<ConsumerInfo> {
        let num_consumers = self.consumers.len();

        if num_consumers == 0 {
            return Err(anyhow!("There are no consumers left"));
        }

        // Use modulo to ensure index wraps around
        let index = self
            .index_consumer
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            % num_consumers;

        // Get the consumer at the computed index
        self.consumers
            .get(index)
            .cloned() // Clone the Arc<Mutex<Consumer>> to return
            .ok_or_else(|| anyhow!("Unable to find the next consumer"))
    }
}
