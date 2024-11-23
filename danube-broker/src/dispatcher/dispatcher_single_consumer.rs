use anyhow::{anyhow, Result};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    RwLock,
};
use tracing::{trace, warn};

use crate::{
    consumer::{Consumer, MessageToSend},
    subscription::ConsumerInfo,
};

use super::DispatcherCommand;

#[derive(Debug)]
pub(crate) struct DispatcherSingleConsumer {
    consumers: Vec<Consumer>,
    active_consumer: RwLock<Option<Consumer>>,
    rx_disp: Receiver<DispatcherCommand>,
    tx_response: Sender<Result<()>>,
}

impl DispatcherSingleConsumer {
    pub(crate) fn new(
        rx_disp: Receiver<DispatcherCommand>,
        tx_response: Sender<Result<()>>,
    ) -> Self {
        DispatcherSingleConsumer {
            consumers: Vec::new(),
            active_consumer: RwLock::new(None),
            rx_disp,
            tx_response,
        }
    }

    pub(crate) async fn run(&self) -> Result<()> {
        loop {
            if let Some(command) = self.rx_disp.recv().await {
                let result = match command {
                    DispatcherCommand::AddConsumer(consumer) => {
                        self.add_consumer(consumer).await;
                    }
                    DispatcherCommand::RemoveConsumer(consumer_id) => {
                        self.remove_consumer(consumer_id).await;
                    }
                    DispatcherCommand::Dispatch(message) => {
                        self.send_messages(message).await;
                    }
                    DispatcherCommand::Shutdown => {
                        break;
                    }
                };
                // Send the result back through the response channel
                if let Err(e) = self.tx_response.send(result).await {
                    warn!("Failed to send dispatcher response: {}", e);
                }
            }
        }
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

    // sending messages to an active consumer
    pub(crate) async fn send_messages(&self, messages: MessageToSend) -> Result<()> {
        // Try to acquire the read lock on the active consumer
        let active_consumer = {
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

        // check if the consumer is active
        if !active_consumer.get_status().await {
            // Pick a new active consumer
            if !self.pick_active_consumer().await {
                return Err(anyhow!(
                    "There is no active consumer to dispatch the message"
                ));
            }
        } else {
            active_consumer.tx_broker.send(messages).await?;
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
            self.active_consumer = Some(consumer).into()
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

    pub(crate) fn get_consumers(&self) -> &Vec<ConsumerInfo> {
        &self.consumers
    }
}
