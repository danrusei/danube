use anyhow::{anyhow, Result};
use metrics::gauge;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use tracing::trace;

use crate::{
    broker_metrics::TOPIC_CONSUMERS,
    consumer::{Consumer, MessageToSend},
    dispatcher::{
        dispatcher_multiple_consumers::DispatcherMultipleConsumers,
        dispatcher_single_consumer::DispatcherSingleConsumer, Dispatcher,
    },
    utils::get_random_id,
};

// Subscriptions manage the consumers that are subscribed to them.
// They also handle dispatchers that manage the distribution of messages to these consumers.
#[derive(Debug)]
pub(crate) struct Subscription {
    pub(crate) subscription_name: String,
    pub(crate) subscription_type: i32,
    pub(crate) topic_name: String,
    // Each consumer has a `mpsc::Sender` channel for sending messages
    pub(crate) consumers: HashMap<u64, (ConsumerInfo, tokio::task::JoinHandle<()>)>,
    pub(crate) dispatcher: Option<Dispatcher>,
}

#[derive(Debug, Clone)]
pub(crate) struct ConsumerInfo {
    pub(crate) consumer_id: u64,
    pub(crate) sub_options: SubscriptionOptions,
    pub(crate) status: Arc<Mutex<bool>>,
    pub(crate) tx_broker: mpsc::Sender<MessageToSend>,
    pub(crate) rx_cons: Arc<Mutex<mpsc::Receiver<MessageToSend>>>,
}

impl ConsumerInfo {
    pub(crate) async fn get_status(&self) -> bool {
        *self.status.lock().await
    }
    pub(crate) async fn set_status_false(&self) -> () {
        *self.status.lock().await = false
    }
    pub(crate) async fn set_status_true(&self) -> () {
        *self.status.lock().await = true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SubscriptionOptions {
    pub(crate) subscription_name: String,
    pub(crate) subscription_type: i32, // should be moved to SubscriptionType
    pub(crate) consumer_id: Option<u64>,
    pub(crate) consumer_name: String,
}

impl Subscription {
    // create new subscription
    pub(crate) fn new(
        sub_options: SubscriptionOptions,
        topic_name: &str,
        _meta_properties: HashMap<String, String>,
    ) -> Self {
        Subscription {
            subscription_name: sub_options.subscription_name,
            subscription_type: sub_options.subscription_type,
            topic_name: topic_name.into(),
            consumers: HashMap::new(),
            dispatcher: None,
        }
    }
    // Adds a consumer to the subscription
    pub(crate) async fn add_consumer(
        &mut self,
        topic_name: &str,
        options: SubscriptionOptions,
    ) -> Result<u64> {
        // for communication with broker
        let (tx_broker, rx_broker) = mpsc::channel(4);
        //for communication with client consumer
        let (tx_cons, rx_cons) = mpsc::channel(4);

        let consumer_id = get_random_id();
        let consumer_status = Arc::new(Mutex::new(true));
        let mut consumer = Consumer::new(
            consumer_id,
            &options.consumer_name,
            options.subscription_type,
            topic_name,
            rx_broker,
            tx_cons,
            consumer_status.clone(),
        );

        // Spawn consumer task
        let handle = tokio::spawn(async move {
            consumer.run().await;
        });

        // checks if there'a a dispatcher (responsible for distributing messages to consumers)
        // if not initialize a new dispatcher based on the subscription type: Exclusive, Shared, Failover
        let dispatcher = if let Some(dispatcher) = self.dispatcher.as_mut() {
            dispatcher
        } else {
            let new_dispatcher = match options.subscription_type {
                // Exclusive
                0 => Dispatcher::OneConsumer(DispatcherSingleConsumer::new()),

                // Shared
                1 => Dispatcher::MultipleConsumers(DispatcherMultipleConsumers::new()),

                // Failover
                2 => Dispatcher::OneConsumer(DispatcherSingleConsumer::new()),

                _ => {
                    return Err(anyhow!("Should not get here"));
                }
            };

            // Set the dispatcher for the subscription
            self.dispatcher = Some(new_dispatcher);

            self.dispatcher.as_mut().unwrap()
        };

        let consumer_info = ConsumerInfo {
            consumer_id,
            sub_options: options,
            status: consumer_status,
            tx_broker,
            rx_cons: Arc::new(Mutex::new(rx_cons)),
        };

        // Insert the consumer into the subscription's consumer list
        self.consumers
            .insert(consumer_id, (consumer_info.clone(), handle));

        // Add the consumer to the dispatcher
        dispatcher.add_consumer(consumer_info).await?;

        trace!(
            "A dispatcher {:?} has been added on subscription {}",
            dispatcher,
            self.subscription_name
        );

        Ok(consumer_id)
    }

    pub(crate) fn get_consumer_rx(
        &self,
        consumer_id: u64,
    ) -> Option<Arc<Mutex<mpsc::Receiver<MessageToSend>>>> {
        if let Some(consumer_info) = self.consumers.get(&consumer_id) {
            return Some(consumer_info.0.rx_cons.clone());
        }
        None
    }

    pub(crate) fn get_consumer_info(&self, consumer_id: u64) -> Option<ConsumerInfo> {
        if let Some(consumer) = self.consumers.get(&consumer_id) {
            return Some(consumer.0.clone());
        }
        None
    }

    // handles the disconnection of consumers associated with the subscription.
    pub(crate) async fn disconnect(&mut self) -> Result<Vec<u64>> {
        let mut consumers_id = Vec::new();

        // in order to disconnect the consumers we have to
        // * abort the the JoinHandle
        // * set Consumer status to false, to inform the client the server side was removed
        // * remove Consumer from the subscription
        // * remove Consumer from the dispatcher
        // * send back to broker service the list of disconnected consumers
        for (_, consumer) in &self.consumers {
            consumer.1.abort();
            consumers_id.push(consumer.0.consumer_id);
            gauge!(TOPIC_CONSUMERS.name, "topic" => self.topic_name.to_string()).decrement(1);
            consumer.0.set_status_false().await;
        }

        for consumer_id in &consumers_id {
            self.consumers.remove(&consumer_id);
            if let Some(dispatcher) = &mut self.dispatcher {
                dispatcher.remove_consumer(consumer_id.clone()).await?;
            }
        }

        Ok(consumers_id)
    }

    // Validate Consumer - returns consumer ID
    pub(crate) async fn validate_consumer(&self, consumer_name: &str) -> Option<u64> {
        for consumer_info in self.consumers.values() {
            if consumer_info.0.sub_options.consumer_name == consumer_name {
                // if consumer exist and its status is false, then the consumer has disconnected
                // the consumer client may try to reconnect
                // then set the status to true and use the consumer
                if !consumer_info.0.get_status().await {
                    consumer_info.0.set_status_true().await;
                }
                return Some(consumer_info.0.consumer_id);
            }
        }
        None
    }

    // Get Consumers
    pub(crate) fn get_consumers(&self) -> Vec<u64> {
        self.consumers.keys().cloned().collect()
    }

    pub(crate) fn is_exclusive(&self) -> bool {
        if self.subscription_type == 0 {
            return true;
        }
        return false;
    }

    // Get Dispatcher
    pub(crate) fn get_dispatcher(&self) -> Option<&Dispatcher> {
        // maybe create  a trait that the both dispatachers will implement
        if let Some(dispatcher) = &self.dispatcher {
            return Some(dispatcher);
        }
        None
    }
}
