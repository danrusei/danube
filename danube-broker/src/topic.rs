use anyhow::{anyhow, Result};
use metrics::counter;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::{info, trace, warn};

use crate::proto::MessageMetadata;
use crate::{
    broker_metrics::{TOPIC_BYTES_IN_COUNTER, TOPIC_MSG_IN_COUNTER},
    consumer::{Consumer, MessageToSend},
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
    // subscription_name -> Subscription
    pub(crate) subscriptions: Mutex<HashMap<String, Subscription>>,
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
            subscriptions: Mutex::new(HashMap::new()),
            producers: HashMap::new(),
        }
    }

    #[allow(unused_assignments)]
    pub(crate) fn create_producer(
        &mut self,
        producer_id: u64,
        producer_name: &str,
        producer_access_mode: i32,
    ) -> Result<serde_json::Value> {
        let mut producer_config = serde_json::Value::String(String::new());
        match self.producers.entry(producer_id) {
            Entry::Vacant(entry) => {
                let new_producer = Producer::new(
                    producer_id,
                    producer_name.into(),
                    self.topic_name.clone(),
                    producer_access_mode,
                );

                producer_config = serde_json::to_value(&new_producer)?;

                entry.insert(new_producer);
            }
            Entry::Occupied(entry) => {
                //let current_producer = entry.get();
                info!("the requested producer: {} already exists", entry.key());
                return Err(anyhow!(" the producer already exist"));
            }
        }

        Ok(producer_config)
    }

    // Close this topic - disconnect all producers and subscriptions associated with this topic
    pub(crate) async fn close(&mut self) -> Result<(Vec<u64>, Vec<u64>)> {
        let mut disconnected_producers = Vec::new();
        let mut disconnected_consumers = Vec::new();

        // Disconnect all the topic producers
        for (_, producer) in self.producers.iter_mut() {
            let producer_id = producer.disconnect();
            disconnected_producers.push(producer_id);
        }

        // Disconnect all the topic subscriptions
        for (_, subscription) in self.subscriptions.lock().await.iter_mut() {
            let mut consumers = subscription.disconnect().await?;
            disconnected_consumers.append(&mut consumers);
        }

        Ok((disconnected_producers, disconnected_consumers))
    }

    // Publishes the message to the topic, and send to active consumers
    pub(crate) async fn publish_message(
        &self,
        producer_id: u64,
        payload: Vec<u8>,
        meta: Option<MessageMetadata>,
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
        match producer.publish_message(producer_id, &payload).await {
            Ok(_) => {
                counter!(TOPIC_MSG_IN_COUNTER.name, "topic"=> self.topic_name.clone() , "producer" => producer_id.to_string()).increment(1);
                counter!(TOPIC_BYTES_IN_COUNTER.name, "topic"=> self.topic_name.clone() , "producer" => producer_id.to_string()).increment(payload.len() as u64);
            }
            Err(err) => {
                return Err(anyhow!("the Producer checks have failed: {}", err));
            }
        }

        let message_to_send = MessageToSend {
            payload,
            metadata: meta,
        };

        // Collect subscriptions that need to be unsubscribed, if contain no active consumers
        let subscriptions_to_remove: Vec<String> = {
            let subscriptions = self.subscriptions.lock().await;
            let mut to_remove = Vec::new();

            for (_name, subscription) in subscriptions.iter() {
                let duplicate_message = message_to_send.clone();
                if let Some(dispatcher) = subscription.get_dispatcher() {
                    if let Err(err) = dispatcher.send_messages(duplicate_message).await {
                        info!(
                            "The subscription {}, has no active consumers, got error: {} ",
                            subscription.subscription_name, err
                        );
                        to_remove.push(subscription.subscription_name.clone());
                    }
                } else {
                    trace!(
                        "No dispatcher has been found for subscription {}",
                        subscription.subscription_name
                    );
                }
            }

            to_remove
        };

        for subscription_name in subscriptions_to_remove {
            self.unsubscribe(&subscription_name).await;

            //TODO! delete the subscription from the metadata store
        }

        Ok(())
    }

    pub(crate) fn get_producer_status(&self, producer_id: u64) -> bool {
        if let Some(producer) = self.producers.get(&producer_id) {
            if producer.status == true {
                return true;
            }
        }
        false
    }

    // Subscribe to the topic and create a consumer for receiving messages
    pub(crate) async fn subscribe(
        &self,
        topic_name: &str,
        options: SubscriptionOptions,
    ) -> Result<Arc<Mutex<Consumer>>> {
        //Todo! sub_metadata is user-defined information to the subscription,
        //maybe for user internal business, management and montoring
        let sub_metadata = HashMap::new();

        // Lock the subscriptions and get the entry
        let mut subscriptions_lock = self.subscriptions.lock().await;
        let subscription = subscriptions_lock
            .entry(options.subscription_name.clone())
            .or_insert(Subscription::new(options.clone(), sub_metadata));

        if subscription.is_exclusive() && !subscription.get_consumers().is_empty() {
            warn!("Not allowed to add the Consumer: {}, the Exclusive subscription can't be shared with other consumers", options.consumer_name);
            return Err(anyhow!("Not allowed to add the Consumer: {}, the Exclusive subscription can't be shared with other consumers", options.consumer_name));
        }

        let consumer_id = get_random_id();

        let consumer = Arc::new(Mutex::new(Consumer::new(
            topic_name,
            consumer_id,
            options.consumer_name.as_str(),
            options.subscription_name.clone().as_str(),
            options.subscription_type,
        )));

        //Todo! Check the topic policies with max_consumers per topic

        // is it ok to clone ? .. or should return just the ID, or ARC
        subscription.add_consumer(consumer.clone()).await?;

        Ok(consumer)
    }

    // Unsubscribes the specified subscription from the topic
    // should be called if all consumers are disconnected
    pub(crate) async fn unsubscribe(&self, subscription_name: &str) {
        let _ = self.subscriptions.lock().await.remove(subscription_name);
    }

    pub(crate) async fn validate_consumer(
        &self,
        subscription_name: &str,
        consumer_name: &str,
    ) -> Option<u64> {
        let sub_guard = self.subscriptions.lock().await;
        let subscription = match sub_guard.get(subscription_name) {
            Some(subscr) => subscr,
            None => return None,
        };

        let consumer_id = match subscription.validate_consumer(consumer_name).await {
            Some(id) => id,
            None => return None,
        };

        Some(consumer_id)
    }

    // check_subscription checks if the subscription is activelly used by any consumer
    pub(crate) async fn check_subscription(&self, subscription: &str) -> Option<bool> {
        let sub_guard = self.subscriptions.lock().await;
        let subs = sub_guard.get(subscription)?;

        let disp = subs.get_dispatcher()?;
        let consumers = disp.get_consumers();

        for consumer in consumers {
            let cons_guard = consumer.lock().await;
            if cons_guard.status {
                return Some(true);
            }
        }

        Some(false)
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
