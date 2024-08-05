use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::trace;

use crate::{
    consumer::Consumer,
    dispatcher::{
        dispatcher_multiple_consumers::DispatcherMultipleConsumers,
        dispatcher_single_consumer::DispatcherSingleConsumer, Dispatcher,
    },
};

// Subscriptions manage the consumers that are subscribed to them.
// They also handle dispatchers that manage the distribution of messages to these consumers.
#[derive(Debug)]
pub(crate) struct Subscription {
    pub(crate) topic_name: String,
    pub(crate) subscription_name: String,
    // the consumers registered to the subscription, consumer_id -> Consumer
    pub(crate) consumers: HashMap<u64, Arc<Mutex<Consumer>>>,
    pub(crate) dispatcher: Option<Dispatcher>,
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
        topic_name: impl Into<String>,
        subscription_name: impl Into<String>,
        _meta_properties: HashMap<String, String>,
    ) -> Self {
        Subscription {
            topic_name: topic_name.into(),
            subscription_name: subscription_name.into(),
            consumers: HashMap::new(),
            dispatcher: None,
        }
    }
    // Adds a consumer to the subscription
    pub(crate) async fn add_consumer(&mut self, consumer: Arc<Mutex<Consumer>>) -> Result<()> {
        // checks if there'a a dispatcher (responsible for distributing messages to consumers)
        // If not initializes a new dispatcher based on the type of consumer: Exclusive, Shared, Failover

        // Retrieve the subscription type and consumer ID without holding the lock for too long
        let (subscription_type, consumer_id);

        {
            let consumer_guard = consumer.lock().await;
            subscription_type = consumer_guard.subscription_type;
            consumer_id = consumer_guard.consumer_id;
        }

        // Initialize the appropriate dispatcher based on the subscription type
        let mut dispatcher = match subscription_type {
            // Exclusive
            0 => Dispatcher::OneConsumer(DispatcherSingleConsumer::new(
                &self.topic_name,
                &self.subscription_name,
                0,
            )),

            // Shared
            1 => Dispatcher::MultipleConsumers(DispatcherMultipleConsumers::new(
                &self.topic_name,
                &self.subscription_name,
            )),

            // Failover
            2 => Dispatcher::OneConsumer(DispatcherSingleConsumer::new(
                &self.topic_name,
                &self.subscription_name,
                2,
            )),

            _ => {
                return Err(anyhow!("Should not get here"));
            }
        };

        // Insert the consumer into the subscription's consumer list
        self.consumers.insert(consumer_id, consumer.clone());

        // Add the consumer to the dispatcher
        dispatcher.add_consumer(consumer.clone()).await?;

        trace!(
            "A dispatcher {:?} has been added on subscription {}",
            dispatcher,
            self.subscription_name
        );

        // Set the dispatcher for the subscription
        self.dispatcher = Some(dispatcher);

        Ok(())
    }

    // remove a consumer from the subscription
    #[allow(dead_code)]
    pub(crate) fn remove_consumer(_consumer: Consumer) -> Result<()> {
        // TODO!
        // removes consumer from the dispatcher
        // If there are no consumers left after removing the specified one,
        // it unsubscribes the subscription from the topic.
        todo!()
    }

    // handles the disconnection of consumers associated with the subscription.
    pub(crate) async fn disconnect(&mut self) -> Result<Vec<u64>> {
        let mut consumers_id = Vec::new();

        if let Some(dispatcher) = &self.dispatcher {
            let mut disconnected_consumers = dispatcher.disconnect_all_consumers().await?;
            consumers_id.append(&mut disconnected_consumers);
        }

        Ok(consumers_id)
    }

    // Deletes the subscription after it is unsubscribed from the topic and disconnected from consumers.
    #[allow(dead_code)]
    pub(crate) fn delete() -> Result<()> {
        // TODO!
        //  It checks if there is a dispatcher associated with the subscription.
        // If there is, it calls the disconnectAllConsumers method of the dispatcher
        todo!()
    }

    // Get Consumer - returns consumer ID
    pub(crate) async fn get_consumer_id(&self, consumer_name: &str) -> Option<u64> {
        for consumer in self.consumers.values() {
            if consumer.lock().await.consumer_name == consumer_name {
                return Some(consumer.lock().await.consumer_id);
            }
        }
        None
    }
    // Get Consumers
    #[allow(dead_code)]
    pub(crate) fn get_consumers() -> Result<Vec<Consumer>> {
        // ask dispatcher
        todo!()
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
