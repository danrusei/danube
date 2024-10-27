use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, RwLock};
use tracing::{trace, warn};

use crate::consumer::MessageToSend;

#[derive(Debug)]
pub(crate) struct DispatcherSingleConsumer {
    consumers: Vec<(u64, mpsc::Sender<MessageToSend>)>,
    active_consumer: RwLock<Option<(u64, mpsc::Sender<MessageToSend>)>>,
}

impl DispatcherSingleConsumer {
    pub(crate) fn new() -> Self {
        DispatcherSingleConsumer {
            consumers: Vec::new(),
            active_consumer: RwLock::new(None),
        }
    }

    // Pick an active consumer for a topic for subscription.
    pub(crate) async fn pick_active_consumer(&self) -> bool {
        // sort the self.consumers ,after a specific logic, maybe highest priority

        let mut candidate = None;

        for consumer in &self.consumers {
            // validates somehow that the consumer has called the receive methods and is valid
            candidate = Some((consumer.0, consumer.1.clone()));
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
        if !active_consumer.status {
            // Pick a new active consumer
            if !self.pick_active_consumer().await {
                return Err(anyhow!(
                    "There is no active consumer to dispatch the message"
                ));
            }
        } else {
            active_consumer.1.send(messages).await?;
        }

        Ok(())
    }

    // manage the addition of consumers to the dispatcher
    pub(crate) async fn add_consumer(
        &mut self,
        consumer_id: u64,
        tx_broker: mpsc::Sender<MessageToSend>,
    ) -> Result<()> {
        // Handle Exclusive Subscription
        // The consumer addition is not allowed if there are consumers in the list and Subscription is Exclusive
        let consumer_subscription_type;

        // if the subscription is Shared should not be routed to this dispatcher
        if consumer_subscription_type == 1 {
            return Err(anyhow!(
                "Erroneous routing, Shared subscription should use dispatcher multiple consumer"
            ));
        }

        if consumer_subscription_type == 0 && !self.consumers.is_empty() {
            // connect to active consumer self.active_consumer
            warn!("Not allowed to add the Consumer: {}, the Exclusive subscription can't be shared with other consumers", consumer_guard.consumer_id);
            return Err(anyhow!("Not allowed to add the Consumer, the Exclusive subscription can't be shared with other consumers"));
        }

        if self.consumers.is_empty() {
            self.active_consumer = Some((consumer_id, tx_broker)).into()
        } else {
            if !self.pick_active_consumer().await {
                return Err(anyhow!("Unable to pick an active Consumer"));
            }
        }

        // add Exclusive and Failover consumer to dispatcher
        self.consumers.push((consumer_id, tx_broker));

        trace!(
            "The dispatcher DispatcherSingleConsumer has added the consumer {}",
            consumer_id
        );

        Ok(())
    }

    pub(crate) async fn disconnect_all_consumers(&self) -> Result<Vec<u64>> {
        let mut consumers = Vec::new();

        for consumer in self.consumers.iter() {
            consumers.push(consumer.0)
        }
        Ok(consumers)
    }

    pub(crate) fn get_consumers(&self) -> Vec<u64> {
        let mut consumer_list = Vec::new();
        for consumer in &self.consumers {
            consumer_list.push(consumer.0);
        }
        consumer_list
    }
}
