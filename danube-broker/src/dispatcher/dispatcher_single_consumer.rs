use anyhow::{anyhow, Result};
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, Mutex, RwLock};
use tracing::{trace, warn};

use crate::consumer::{Consumer, MessageToSend};

#[derive(Debug)]
pub(crate) struct DispatcherSingleConsumer {
    consumers: Vec<Consumer>,
    active_consumer: RwLock<Option<Consumer>>,
    rx_disp: Arc<Mutex<Receiver<MessageToSend>>>,
}

impl DispatcherSingleConsumer {
    pub(crate) fn new(rx_disp: Arc<Mutex<Receiver<MessageToSend>>>) -> Self {
        DispatcherSingleConsumer {
            consumers: Vec::new(),
            active_consumer: RwLock::new(None),
            rx_disp,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        loop {
            let mut rx = self.rx_disp.lock().await;
            if let Some(_message) = rx.recv().await {
                todo!();
            };
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
            self.active_consumer = Some(consumer.clone()).into()
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
