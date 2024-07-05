use anyhow::{anyhow, Result};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::trace;

use crate::{
    consumer::Consumer,
    policies::Policies,
    producer::Producer,
    schema::Schema,
    subscription::{Subscription, SubscriptionOptions},
    utils::get_random_id,
};

pub(crate) static SYSTEM_TOPIC: &str = "/system/_events_topic";

// Topic
//
// Manage its own producers and subscriptions. This includes maintaining the state of producers
// and subscriptions and handling message publishing and consumption.
//
// Topics are responsible for accepting messages from producers
// and ensuring they are delivered to the correct subscriptions.
//
// Topics string representation:  /{namespace}/{topic-name}
//
#[derive(Debug)]
pub(crate) struct Topic {
    pub(crate) topic_name: String,
    pub(crate) schema: Option<Schema>,
    pub(crate) topic_policies: Option<Policies>,
    // subscription_id -> Subscription
    pub(crate) subscriptions: HashMap<String, Subscription>,
    // the producers currently connected to this topic, producer_id -> Producer
    pub(crate) producers: HashMap<u64, Producer>,
}

// TODO! should be moved to support partitioned topics from scratch, in order to support hashing dispatch

impl Topic {
    pub(crate) fn new(topic_name: &str) -> Self {
        Topic {
            topic_name: topic_name.into(),
            schema: None,
            topic_policies: None,
            subscriptions: HashMap::new(),
            producers: HashMap::new(),
        }
    }

    // Close this topic - disconnect all producers and subscriptions associated with this topic
    pub(crate) fn close(&self) -> Result<(Vec<u64>, Vec<u64>)> {
        // TODO! disconnect all the topic producers

        // TODO! close all the topic subscriptions
        todo!()
    }

    // Publishes the message to the topic, and send to active consumers
    pub(crate) async fn publish_message(
        &self,
        producer_id: u64,
        message_sequence_id: u64,
        message: Vec<u8>,
    ) -> Result<()> {
        let producer = if let Some(top) = self.producers.get(&producer_id) {
            top
        } else {
            return Err(anyhow!(
                "the producer with id {} is not attached to topic name: {}",
                producer_id,
                self.topic_name
            ));
        };

        //TODO! this is doing nothing for now, and may not need to be async
        match producer
            .publish_message(producer_id, message_sequence_id, &message)
            .await
        {
            Ok(_) => (),
            Err(err) => {
                return Err(anyhow!("the Producer checks have failed: {}", err));
            }
        }

        // let data: Bytes = message.into();

        // Dispatch message to all consumers

        for (_name, subscription) in self.subscriptions.iter() {
            let duplicate_data = message.clone();
            if let Some(dispatcher) = subscription.get_dispatcher() {
                trace!(
                    "A dispatcher was found for the subscription {}",
                    subscription.subscription_name
                );
                dispatcher.send_messages(duplicate_data).await?;
            } else {
                trace!(
                    "No dispatcher has been found for subscription {}",
                    subscription.subscription_name
                )
            }
        }

        Ok(())
    }

    // Subscribe to the topic and create a consumer for receiving messages
    pub(crate) async fn subscribe(
        &mut self,
        topic_name: &str,
        options: SubscriptionOptions,
    ) -> Result<Arc<Mutex<Consumer>>> {
        //Todo! sub_metadata is user-defined information to the subscription, maybe for user internal business, management and montoring
        let sub_metadata = HashMap::new();
        let subscription = self
            .subscriptions
            .entry(options.subscription_name.clone())
            .or_insert(Subscription::new(
                topic_name,
                options.subscription_name.clone(),
                sub_metadata,
            ));

        let consumer_id = get_random_id();

        let consumer = Arc::new(Mutex::new(Consumer::new(
            topic_name,
            consumer_id,
            options.consumer_name.as_str(),
            options.subscription_name.clone().as_str(),
            options.subscription_type,
        )));

        //Todo! Check the topic policies with max_consumers per topic

        // is it ok to clone , or I should return just the ID, or ARC?
        subscription.add_consumer(consumer.clone()).await?;

        Ok(consumer)
    }

    // Unsubscribes the specified subscription from the topic
    #[allow(dead_code)]
    pub(crate) fn unsubscribe(&self, _subscription_name: &str) -> Result<()> {
        todo!()
    }

    pub(crate) fn get_subscription(&self, subscription_name: &str) -> Option<&Subscription> {
        self.subscriptions.get(subscription_name)
    }

    // Update Topic Policies
    pub(crate) fn policies_update(&mut self, policies: Policies) -> Result<()> {
        self.topic_policies = Some(policies);

        Ok(())
    }

    // Add a schema to the topic.
    pub(crate) fn add_schema(&mut self, schema: Schema) -> Result<()> {
        self.schema = Some(schema);
        Ok(())
    }

    // get topic's schema
    #[allow(dead_code)]
    pub(crate) fn get_schema(&self) -> Option<Schema> {
        self.schema.clone()
    }

    // Add a schema to the topic.
    #[allow(dead_code)]
    pub(crate) fn delete_schema(&self, _schema: Schema) -> Result<()> {
        todo!()
    }
}
