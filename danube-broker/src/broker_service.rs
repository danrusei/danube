use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use std::any;
use std::collections::{hash_map::Entry, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;

use crate::proto::{ProducerAccessMode, Schema};
use crate::{
    consumer::Consumer,
    producer::Producer,
    subscription::SubscriptionOptions,
    topic::{self, Topic},
    utils::get_random_id,
};

// BrokerService - owns the topics and  and manage their lifecycle.
// adn facilitate the creation of producers and subscriptions.
#[derive(Debug)]
pub(crate) struct BrokerService {
    //broker_addr: SocketAddr,
    // a map with namespace wise topics - Namespace --> topicName --> topic
    //topics: DashMap<String, DashMap<String, Topic>>,
    // topic_name to Topic
    pub(crate) topics: HashMap<String, Topic>,
    // brokers to register listeners for configuration changes or updates
    // config_listeners: DashMap<String, Consumer>,
    // the list of active producers, mapping producer_id to topic_name
    pub(crate) producers: HashMap<u64, String>,
    // the list of active consumers
    pub(crate) consumers: HashMap<u64, String>,
}

impl BrokerService {
    pub(crate) fn new() -> Self {
        BrokerService {
            //broker_addr: broker_addr,
            //topics: DashMap::new(),
            //config_listeners: DashMap::new,
            topics: HashMap::new(),
            producers: HashMap::new(),
            consumers: HashMap::new(),
        }
    }

    pub(crate) fn find_or_create_topic(
        &mut self,
        topic_name: &str,
        schema: Option<Schema>,
        create_if_missing: bool,
    ) -> Result<String> {
        if self.topics.contains_key(topic_name) {
            return Ok(topic_name.into());
        }

        if !validate_topic(topic_name) {
            return Err(anyhow!("The topic name has invalid format"));
        }

        // If the topic does not exist and create_if_missing is false
        if !create_if_missing {
            return Err(anyhow!(
                "The topic does not exist and not allowed to be created"
            ));
        }

        let mut new_topic = Topic::new(topic_name);
        if let Some(schema) = schema {
            new_topic.add_schema(schema);
            //Todo! save Schema to metadata Store, use topic resources to get and put schema
        }
        self.topics.insert(topic_name.into(), new_topic);

        Ok(topic_name.into())
    }

    // pub(crate) fn get_topic(
    //     &mut self,
    //     topic_name: String,
    //     create_if_missing: bool,
    // ) -> Result<&mut Topic> {
    //     // Check if the topic exists first
    //     if self.topics.contains_key(&topic_name) {
    //         return Ok(self.topics.get_mut(&topic_name).unwrap());
    //     }

    //     // If the topic does not exist and create_if_missing is true
    //     if create_if_missing {
    //         let new_topic = topic::Topic::new(topic_name.clone());
    //         self.topics.insert(topic_name.clone(), new_topic);
    //         return Ok(self.topics.get_mut(&topic_name).unwrap());
    //     }

    //     // If the topic does not exist and create_if_missing is false, return an error
    //     Err(anyhow!("unable to create the topic"))
    // }

    pub(crate) async fn delete_topic(&mut self, topic: String) -> Result<Topic> {
        todo!()
    }

    pub(crate) fn check_if_producer_exist(
        &self,
        topic_name: String,
        producer_name: String,
    ) -> bool {
        let topic = self
            .topics
            .get(&topic_name)
            .expect("unable to find the topic");
        for producer in topic.producers.values() {
            if producer.producer_name == producer_name {
                return true;
            }
        }
        false
    }

    pub(crate) fn check_if_consumer_exist(
        &self,
        consumer_name: &str,
        subscription_name: &str,
        topic_name: &str,
    ) -> Option<u64> {
        // assuming that we know that the topic exist
        let topic = self
            .topics
            .get(topic_name)
            .expect("unable to find the topic");

        let subscription = match topic.get_subscription(subscription_name) {
            Some(subscr) => subscr,
            None => return None,
        };

        let consumer_id = match subscription.get_consumer_id(consumer_name) {
            Some(id) => id,
            None => return None,
        };

        Some(consumer_id)
    }

    pub(crate) fn allow_subscription_creation(&self, topic_name: impl Into<String>) -> bool {
        // check the topic policies here
        let _topic = self
            .topics
            .get(&topic_name.into())
            .expect("unable to find the topic");

        //TODO! once the policies Topic&Namespace Policies are in place we can verify if it is allowed

        true
    }

    pub(crate) fn subscribe(
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

        let consumer = topic
            .subscribe(topic_name, subscription_options)
            .expect("should work");

        Ok(consumer.consumer_id)
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
                return Err(anyhow!(" the producer already exist"));
            }
        }

        self.producers
            .entry(producer_id)
            .or_insert(topic_name.into());

        Ok(producer_id)
    }

    // knowing the producer_id, return to the caller the topic associated to the producer
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

    // pub(crate) async fn register_configuration_listener(
    //     &mut self,
    //     config_key: String,
    //     listener: Consumer,
    // ) {
    //     self.config_listeners.insert(config_key, listener);
    // }
}

fn validate_topic(input: &str) -> bool {
    // length from 5 to 20 characters
    if input.len() < 5 || input.len() > 20 {
        return false;
    }

    // allowed characters are letters, numbers and "_"
    for ch in input.chars() {
        if !ch.is_alphanumeric() && ch != '_' {
            return false;
        }
    }

    true
}
