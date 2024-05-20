use bytes::BytesMut;
use dashmap::DashMap;
use std::{collections::HashMap, error::Error, sync::Arc};

use danube_util::schema::Schema;

use crate::{
    broker_service::{self, BrokerService},
    consumer::Consumer,
    producer::Producer,
    subscription::{Subscription, SubscriptionOption},
};

#[derive(Debug)]
pub(crate) struct Topic {
    topic_name: String,
    topic_policies: TopicPolicies,
    subscriptions: HashMap<String, Subscription>,
    // Producers currently connected to this topic
    producers: HashMap<String, Producer>,
}

impl Topic {
    pub(crate) fn new(topic_name: String) -> Self {
        Topic {
            topic_name,
            topic_policies: TopicPolicies::default(),
            subscriptions: HashMap::new(),
            producers: HashMap::new(),
        }
    }

    // Close all producers and subscriptions associated with this topic
    pub(crate) fn close() -> Result<(), Box<dyn Error>> {
        todo!()
    }

    // Close all producers/consumers and deletes the topic
    pub(crate) fn delete() -> Result<(), Box<dyn Error>> {
        todo!()
    }

    // Publishes a message to the topic
    pub(crate) async fn publish_message(data: BytesMut) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    // Create a new subscription for the topic
    pub(crate) async fn create_subscription(
        subscription_name: String,
        properties: HashMap<String, String>,
    ) -> Result<Consumer, Box<dyn Error>> {
        todo!()
    }

    // Subscribe to the topic and create a consumer for receiving messages
    pub(crate) async fn subscribe(options: SubscriptionOption) -> Result<Consumer, Box<dyn Error>> {
        todo!()
    }

    // Unsubscribes the specified subscription from the topic
    pub(crate) async fn unsubscribe(subscription_name: String) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    // Update Topic Policies
    pub(crate) fn policies_update(policies: TopicPolicies) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    // Add a schema to the topic.
    pub(crate) fn add_schema(schema: Schema) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    // Add a schema to the topic.
    pub(crate) fn delete_schema(schema: Schema) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}

#[derive(Debug, Default)]
pub(crate) struct TopicPolicies {}
