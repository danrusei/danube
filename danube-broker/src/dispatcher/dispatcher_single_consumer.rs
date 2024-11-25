use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, Mutex, RwLock};
use tracing::{trace, warn};

use crate::consumer::{Consumer, MessageToSend};

#[derive(Debug, Clone)]
pub(crate) struct DispatcherSingleConsumer {
    consumers: Vec<Consumer>,
    active_consumer: Arc<RwLock<Option<Consumer>>>,
    rx_disp: Arc<Mutex<Receiver<MessageToSend>>>,
}

impl DispatcherSingleConsumer {
    pub(crate) fn new(rx_disp: Arc<Mutex<Receiver<MessageToSend>>>) -> Self {
        DispatcherSingleConsumer {
            consumers: Vec::new(),
            active_consumer: Arc::new(RwLock::new(None)),
            rx_disp,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        let rx_disp_cloned = self.rx_disp.clone();
        let dispatcher = Arc::new(self.clone());
        tokio::spawn(async move {
            loop {
                let mut rx = rx_disp_cloned.lock().await;
                if let Some(message) = rx.recv().await {
                    if let Err(error) = dispatcher.send_messages(message).await {
                        warn!("Error sending messages: {}", error);
                    }
                };
            }
        });
        Ok(())
    }

    // sending messages to an active consumer
    pub(crate) async fn send_messages(&self, message: MessageToSend) -> Result<()> {
        // Try to acquire the read lock on the active consumer
        let mut active_consumer = {
            let guard = self.active_consumer.read().await;
            match &*guard {
                Some(consumer) => consumer.clone(),
                None => return Err(anyhow::anyhow!("There is no active Consumer")),
            }
        };

        //Todo!
        // 1. check first if the Consumer allow to send the messages
        // 2. filter the messages for consumers
        // 3. other permits like dispatch rate limiter, quota etc

        let consumer_status = active_consumer.status.lock().await;

        // check if the consumer is active
        if !consumer_status.to_owned() {
            // Pick a new active consumer
            if !self.pick_active_consumer().await {
                return Err(anyhow!(
                    "There is no active consumer to dispatch the message"
                ));
            }
        } else {
            drop(consumer_status);
            active_consumer.send_message(message).await?;
        }

        Ok(())
    }

    // manage the addition of consumers to the dispatcher
    pub(crate) async fn add_consumer(&mut self, consumer: Consumer) -> Result<()> {
        // Handle Exclusive Subscription
        // The consumer addition is not allowed if there are consumers in the list and Subscription is Exclusive

        // if the subscription is Shared should not be routed to this dispatcher
        if consumer.subscription_type == 1 {
            return Err(anyhow!(
                "Erroneous routing, Shared subscription should use dispatcher multiple consumer"
            ));
        }

        if consumer.subscription_type == 0 && !self.consumers.is_empty() {
            // connect to active consumer self.active_consumer
            warn!("Not allowed to add the Consumer: {}, the Exclusive subscription can't be shared with other consumers", consumer.consumer_id);
            return Err(anyhow!("Not allowed to add the Consumer, the Exclusive subscription can't be shared with other consumers"));
        }

        if self.consumers.is_empty() {
            self.active_consumer = Arc::new(Some(consumer.clone()).into())
        } else {
            if !self.pick_active_consumer().await {
                return Err(anyhow!("Unable to pick an active Consumer"));
            }
        }

        trace!(
            "The dispatcher DispatcherSingleConsumer has added the consumer {}",
            consumer.consumer_name
        );

        // add Exclusive and Failover consumer to dispatcher
        self.consumers.push(consumer);

        Ok(())
    }

    // Pick an active consumer for a topic for subscription.
    pub(crate) async fn pick_active_consumer(&self) -> bool {
        // sort the self.consumers ,after a specific logic, maybe highest priority

        let mut candidate = None;

        for consumer_info in &self.consumers {
            if consumer_info.get_status().await {
                candidate = Some(consumer_info.clone());
                break;
            }
        }

        if let Some(consumer) = candidate {
            let mut active_consumer = self.active_consumer.write().await;
            *active_consumer = Some(consumer);
            true
        } else {
            false
        }
    }

    pub(crate) async fn disconnect_all_consumers(&mut self) -> Result<Vec<u64>> {
        let mut consumers = Vec::new();

        for consumer in self.consumers.iter() {
            consumers.push(consumer.consumer_id)
        }

        for consumer_id in consumers.iter() {
            self.remove_consumer(consumer_id.clone()).await?;
        }

        Ok(consumers)
    }

    pub(crate) async fn remove_consumer(&mut self, consumer_id: u64) -> Result<()> {
        // Find and disconnect the consumer before removing it
        if let Some(consumer) = self
            .consumers
            .iter_mut()
            .find(|c| c.consumer_id == consumer_id)
        {
            consumer.disconnect().await;
        }

        self.consumers
            .retain(|consumer| consumer.consumer_id != consumer_id);

        // Acquire a write lock on active_consumer to modify it
        let mut active_consumer = self.active_consumer.write().await;

        // Check if the active_consumer matches the consumer_id and set to None if so
        if let Some(ref act_consumer) = *active_consumer {
            if act_consumer.consumer_id == consumer_id {
                *active_consumer = None;
            }
        }

        Ok(())
    }

    pub(crate) fn get_consumers(&self) -> &Vec<Consumer> {
        &self.consumers
    }
}
