use anyhow::Result;
use bytes::BytesMut;
use dashmap::DashMap;
use std::{collections::HashMap, error::Error, sync::Arc};

use danube_util::schema::Schema;

use crate::{
    broker_service::{self, BrokerService},
    consumer::Consumer,
    policies::Policies,
    producer::Producer,
    subscription::{Subscription, SubscriptionOption},
};

#[derive(Debug)]
pub(crate) struct Topic {
    topic_name: String,
    topic_policies: Option<Policies>,
    subscriptions: HashMap<String, Subscription>,
    // Producers currently connected to this topic
    producers: HashMap<String, Producer>,
}

impl Topic {
    pub(crate) fn new(topic_name: String) -> Self {
        Topic {
            topic_name,
            topic_policies: None,
            subscriptions: HashMap::new(),
            producers: HashMap::new(),
        }
    }

    pub(crate) fn initialize(&mut self) -> Result<()> {
        //check for namespace policies and apply to topic using namespace resources getpolicies
        //if found apply namespace policies at topic level
        // if none :
        self.topic_policies = Some(Policies::new());

        Ok(())
    }

    // Close all producers and subscriptions associated with this topic
    pub(crate) fn close() -> Result<()> {
        todo!()
    }

    // Close all producers/consumers and deletes the topic
    pub(crate) fn delete() -> Result<()> {
        todo!()
    }

    // Publishes a message to the topic
    pub(crate) async fn publish_message(data: BytesMut) -> Result<()> {
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
    pub(crate) async fn subscribe(options: SubscriptionOption) -> Result<Consumer> {
        todo!()
    }

    // Unsubscribes the specified subscription from the topic
    pub(crate) async fn unsubscribe(subscription_name: String) -> Result<()> {
        todo!()
    }

    // Update Topic Policies
    pub(crate) fn policies_update(policies: Policies) -> Result<()> {
        todo!()
    }

    // Add a schema to the topic.
    pub(crate) fn add_schema(schema: Schema) -> Result<()> {
        todo!()
    }

    // Add a schema to the topic.
    pub(crate) fn delete_schema(schema: Schema) -> Result<()> {
        todo!()
    }
}
