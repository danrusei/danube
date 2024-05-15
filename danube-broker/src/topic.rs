use bytes::BytesMut;
use dashmap::DashMap;
use std::{collections::HashMap, error::Error};

use danube_util::schema::Schema;

use crate::{
    consumer::Consumer,
    producer::Producer,
    subscription::{Subscription, SubscriptionOption},
};

#[derive(Debug, Default)]
pub(crate) struct Topic {
    topic_name: String,
    topic_policies: TopicPolicies,
    subscriptions: DashMap<String, Subscription>,
    // Producers currently connected to this topic
    producers: DashMap<String, Producer>,
}

impl Topic {
    pub(crate) fn initialize() -> Self {
        Topic::default()
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
