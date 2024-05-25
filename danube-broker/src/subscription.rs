use anyhow::Result;
use std::collections::HashMap;

use crate::{
    consumer::Consumer,
    dispatcher::{
        dispatcher_multiple_consumers::DispatcherMultipleConsumers,
        dispatcher_single_consumer::DispatcherSingleConsumer,
    },
};

use crate::proto::consumer_request::SubscriptionType;

#[derive(Debug, Default)]
pub(crate) struct Subscription {
    pub(crate) topic_name: String,
    pub(crate) subscription_name: String,
    // the consumers registered to the subscription, consumer_id -> Consumer
    pub(crate) consumers: Option<HashMap<String, Consumer>>,
    dispatcher: Option<DispatcherSelect>,
}

#[derive(Debug)]
pub(crate) enum DispatcherSelect {
    OneConsumer(DispatcherSingleConsumer),
    MultipleConsumers(DispatcherMultipleConsumers),
}

#[derive(Debug, Clone)]
pub(crate) struct SubscriptionOptions {
    pub(crate) subscription_name: String,
    pub(crate) subscription_type: i32, // should be moved to SubscriptionType
    pub(crate) consumer_id: u64,
    pub(crate) consumer_name: String,
}

impl Subscription {
    // create new subscription
    pub(crate) fn new(
        topic_name: impl Into<String>,
        subscription_name: impl Into<String>,
        meta_properties: HashMap<String, String>,
    ) -> Self {
        Subscription {
            topic_name: topic_name.into(),
            subscription_name: subscription_name.into(),
            consumers: None,
            dispatcher: None,
        }
    }
    // add a consumer to the subscription
    // checks if there'a a dispatcher (responsible for distributing messages to consumers)
    // If not initializes a dispatcher based on the type of consumer: Exclusive, Shared, Failover, Key_Shared
    pub(crate) async fn add_consumer(consumer: Consumer) -> Result<()> {
        todo!()
    }

    // remove a consumer from the subscription
    pub(crate) async fn remove_consumer(consumer: Consumer) -> Result<()> {
        // removes consumer from the dispatcher
        //If there are no consumers left after removing the specified one,
        // it unsubscribes the subscription from the topic.
        todo!()
    }

    // handles the disconnection of consumers associated with the subscription.
    pub(crate) async fn disconnect() -> Result<()> {
        //  It checks if there is a dispatcher associated with the subscription.
        // If there is, it calls the disconnectAllConsumers method of the dispatcher
        todo!()
    }

    // Deletes the subscription after it is unsubscribed from the topic and disconnected from consumers.
    pub(crate) async fn delete() -> Result<()> {
        //  It checks if there is a dispatcher associated with the subscription.
        // If there is, it calls the disconnectAllConsumers method of the dispatcher
        todo!()
    }

    // Get Consumers
    pub(crate) async fn get_consumers() -> Result<Vec<Consumer>> {
        // ask dispatcher
        todo!()
    }

    // Get Dispatcher
    pub(crate) fn get_dispatcher(&self) -> Option<DispatcherSingleConsumer> {
        // maybe create  a trait that the both dispatachers will implement
        todo!()
    }
}
