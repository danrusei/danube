use anyhow::Result;
use bytes::BytesMut;
use dashmap::DashMap;
use std::{collections::HashMap, error::Error, sync::Arc};

use crate::proto::Schema;

use crate::{
    broker_service::{self, BrokerService},
    consumer::Consumer,
    policies::Policies,
    producer::Producer,
    subscription::{Subscription, SubscriptionOption},
};

#[derive(Debug)]
pub(crate) struct Topic {
    pub(crate) topic_name: String,
    pub(crate) schema: Option<Schema>,
    pub(crate) topic_policies: Option<Policies>,
    // the subscriptions attached on the topic, subscription_id -> Subscription
    pub(crate) subscriptions: HashMap<String, Subscription>,
    // the producers currently connected to this topic, producer_id -> Producer
    pub(crate) producers: HashMap<u64, Producer>,
}

impl Topic {
    pub(crate) fn new(topic_name: String) -> Self {
        Topic {
            topic_name,
            schema: None,
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
    pub(crate) fn add_schema(&mut self, schema: Schema) -> Result<()> {
        self.schema = Some(schema);
        Ok(())
    }

    // Add a schema to the topic.
    pub(crate) fn delete_schema(&self, schema: Schema) -> Result<()> {
        todo!()
    }
}
