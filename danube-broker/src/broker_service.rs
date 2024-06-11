use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use std::any;
use std::collections::{hash_map::Entry, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tracing::info;

use crate::proto::{ProducerAccessMode, Schema};
use crate::{
    consumer::Consumer,
    producer::Producer,
    resources::Resources,
    subscription::SubscriptionOptions,
    topic::{self, Topic},
    utils::get_random_id,
};

// BrokerService - owns the topics and  and manage their lifecycle.
// adn facilitate the creation of producers and subscriptions.
#[derive(Debug)]
pub(crate) struct BrokerService {
    // the id of the broker
    pub(crate) broker_id: u64,
    // to handle the metadata and configurations
    pub(crate) resources: Resources,
    // a map with namespace wise topics - Namespace --> topicName --> topic
    //topics: DashMap<String, DashMap<String, Topic>>,
    // maps topic_name to Topic
    pub(crate) topics: HashMap<String, Topic>,
    // the list of active producers, mapping producer_id to topic_name
    pub(crate) producers: HashMap<u64, String>,
    // the list of active consumers
    pub(crate) consumers: HashMap<u64, Arc<Mutex<Consumer>>>,
}

impl BrokerService {
    pub(crate) fn new(resources: Resources) -> Self {
        let broker_id = get_random_id();
        BrokerService {
            broker_id,
            resources: resources,
            topics: HashMap::new(),
            producers: HashMap::new(),
            consumers: HashMap::new(),
        }
    }

    // get the Topic name or create a new one if create_if_missing is true
    pub(crate) fn get_topic(
        &mut self,
        topic_name: &str,
        schema: Option<Schema>,
        create_if_missing: bool,
    ) -> Result<String> {
        // The topic format is /{namespace_name}/{topic_name}
        if !validate_topic(topic_name) {
            return Err(anyhow!(
                "The topic has an invalid format, should be: /namespace_name/topic_name"
            ));
        }

        if self.topics.contains_key(topic_name) {
            return Ok(topic_name.into());
        }

        // If the topic does not exist and create_if_missing is true
        if create_if_missing {
            if schema.is_none() {
                return Err(anyhow!(
                    "Unable to create a topic without specifying the Schema"
                ));
            }
            let new_topic_name = self.create_new_topic(topic_name, schema.unwrap())?;
            return Ok(new_topic_name);
        }

        // If the topic does not exist and create_if_missing is false, return an error
        Err(anyhow!("Unable to find the topic: {}", topic_name))
    }

    // get all the topics currently served by the Broker
    pub(crate) fn get_topics(&self) -> Vec<&String> {
        let topics: Vec<&String> = self.topics.iter().map(|topic| topic.0).collect();
        topics
    }

    // creates a new topic
    pub(crate) fn create_new_topic(&mut self, topic_name: &str, schema: Schema) -> Result<String> {
        let mut new_topic = Topic::new(topic_name);

        new_topic.add_schema(schema);
        //Todo! save Schema to metadata Store, use topic resources to get and put schema

        self.topics.insert(topic_name.into(), new_topic);

        Ok(topic_name.into())
    }

    // deletes the topic
    pub(crate) async fn delete_topic(&mut self, topic: String) -> Result<Topic> {
        todo!()
    }

    pub(crate) fn check_if_producer_exist(
        &self,
        topic_name: String,
        producer_name: String,
    ) -> bool {
        // the topic is already checked
        let topic = self
            .topics
            .get(&topic_name)
            .expect("The topic should be validated before calling this function");
        for producer in topic.producers.values() {
            if producer.producer_name == producer_name {
                return true;
            }
        }
        false
    }

    // create a new producer and attach to the topic
    pub(crate) fn create_new_producer(
        &mut self,
        producer_name: &str,
        topic_name: &str,
        producer_access_mode: i32,
    ) -> Result<u64> {
        let topic = self
            .topics
            .get_mut(topic_name)
            .expect("we know that the topic exist");

        let producer_id = get_random_id();
        match topic.producers.entry(producer_id) {
            Entry::Vacant(entry) => {
                entry.insert(Producer::new(
                    producer_id,
                    producer_name.into(),
                    topic_name.into(),
                    producer_access_mode,
                ));
            }
            Entry::Occupied(entry) => {
                //let current_producer = entry.get();
                info!("the requested producer: {} already exists", entry.key());
                return Err(anyhow!(" the producer already exist"));
            }
        }

        self.producers
            .entry(producer_id)
            .or_insert(topic_name.into());

        Ok(producer_id)
    }

    // having the producer_id, return to the caller the topic attached to the producer
    pub(crate) fn get_topic_for_producer(&mut self, producer_id: u64) -> Result<&Topic> {
        let topic_name = match self.producers.entry(producer_id) {
            Entry::Vacant(entry) => {
                return Err(anyhow!(
                    "the producer with id {} does not exist",
                    producer_id
                ))
            }
            Entry::Occupied(entry) => entry.get().to_owned(),
        };

        if !self.topics.contains_key(&topic_name) {
            return Err(anyhow!(
                "Unable to find any topic associated to this producer"
            ));
        }

        Ok(self.topics.get_mut(&topic_name).unwrap())
    }

    pub(crate) fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Mutex<Consumer>>> {
        self.consumers.get(&consumer_id).cloned()
    }

    pub(crate) async fn check_if_consumer_exist(
        &self,
        consumer_name: &str,
        subscription_name: &str,
        topic_name: &str,
    ) -> Option<u64> {
        let topic = self
            .topics
            .get(topic_name)
            .expect("The topic should be validated before calling this function");

        let subscription = match topic.get_subscription(subscription_name) {
            Some(subscr) => subscr,
            None => return None,
        };

        let consumer_id = match subscription.get_consumer_id(consumer_name).await {
            Some(id) => id,
            None => return None,
        };

        Some(consumer_id)
    }

    //validate if the consumer is allowed to create new subscription
    pub(crate) fn allow_subscription_creation(&self, topic_name: impl Into<String>) -> bool {
        // check the topic policies here
        let _topic = self
            .topics
            .get(&topic_name.into())
            .expect("unable to find the topic");

        //TODO! once the policies Topic&Namespace Policies are in place we can verify if it is allowed

        true
    }

    //consumer subscribe to topic
    pub(crate) async fn subscribe(
        &mut self,
        topic_name: &str,
        subscription_options: SubscriptionOptions,
    ) -> Result<u64> {
        let topic = self
            .topics
            .get_mut(topic_name)
            .expect("the topic should be there");

        //TODO! use NameSpace service to checkTopicOwnership
        // if it's owened by this instance continue,
        // otherwise communicate to client that it has to do Lookup request, as the topic is not serve by this broker

        let consumer = topic.subscribe(topic_name, subscription_options).await?;

        let consumer_id = consumer.lock().await.consumer_id;

        self.consumers.entry(consumer_id).or_insert(consumer);

        Ok(consumer_id)
    }
}

// Topics string representation:  /{namespace}/{topic-name}
fn validate_topic(input: &str) -> bool {
    let parts: Vec<&str> = input.split('/').collect();

    if parts.len() != 3 {
        return false;
    }

    for part in parts.iter() {
        if !part
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return false;
        }
    }

    true
}
