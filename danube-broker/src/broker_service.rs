use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use rand::Rng;
use std::any;
use std::collections::{hash_map::Entry, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;

use crate::proto::{ProducerAccessMode, Schema};
use crate::subscription::SubscriptionOptions;
use crate::topic::{self, Topic};
use crate::{consumer::Consumer, producer::Producer};

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

    pub(crate) fn get_topic(
        &mut self,
        topic_name: String,
        create_if_missing: bool,
    ) -> Result<&mut Topic> {
        // Check if the topic exists first
        if self.topics.contains_key(&topic_name) {
            return Ok(self.topics.get_mut(&topic_name).unwrap());
        }

        // If the topic does not exist and create_if_missing is true
        if create_if_missing {
            let new_topic = topic::Topic::new(topic_name.clone());
            self.topics.insert(topic_name.clone(), new_topic);
            return Ok(self.topics.get_mut(&topic_name).unwrap());
        }

        // If the topic does not exist and create_if_missing is false, return an error
        Err(anyhow!("unable to create the topic"))
    }

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
        consumer_name: impl Into<String>,
        subscription_name: impl Into<String>,
        topic_name: impl Into<String>,
    ) -> bool {
        let topic = self
            .topics
            .get(&topic_name.into())
            .expect("unable to find the topic");
        let subscription_name = subscription_name.into();
        let consumer_name = consumer_name.into();
        for subscription in topic.subscriptions.values() {
            if subscription.subscription_name == subscription_name {
                for consumer in subscription.consumers.values() {
                    if consumer.consumer_name == consumer_name {
                        return true;
                    }
                }
            }
        }
        false
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
        &self,
        topic_name: impl Into<String>,
        subscription_options: SubscriptionOptions,
    ) -> Result<()> {
        let topic = self
            .topics
            .get(&topic_name.into())
            .expect("the topic should be there");

        let _consumer = topic.subscribe(subscription_options).expect("should work");

        Ok(())
    }

    // create a new producer and attach to the topic
    pub(crate) fn create_new_producer(
        &mut self,
        producer_name: String,
        topic_name: String,
        producer_access_mode: i32,
    ) -> Result<&Producer> {
        let topic = self
            .topics
            .get_mut(&topic_name)
            .expect("we know that the topic exist");
        let producer_id = get_random_id();
        match topic.producers.entry(producer_id) {
            Entry::Vacant(entry) => {
                entry.insert(Producer::new(
                    producer_id,
                    producer_name,
                    topic_name.clone(),
                    producer_access_mode,
                ));
            }
            Entry::Occupied(entry) => {
                //let current_producer = entry.get();
                return Err(anyhow!(" the producer already exist"));
            }
        }

        self.producers.entry(producer_id).or_insert(topic_name);

        Ok(topic
            .producers
            .get(&producer_id)
            .expect("the producer was created above"))
    }

    // knowing the producer_id, return to the caller the producer if exist
    // for now it returns both, producer and topic, we may want to return
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

        let topic = match self.get_topic(topic_name.clone(), false) {
            Ok(topic) => topic,
            Err(err) => {
                return Err(anyhow!(
                    "unable to find the topic associated with producer_id {}",
                    producer_id
                ))
            }
        };

        Ok(topic)
    }

    // pub(crate) async fn register_configuration_listener(
    //     &mut self,
    //     config_key: String,
    //     listener: Consumer,
    // ) {
    //     self.config_listeners.insert(config_key, listener);
    // }
}

fn get_random_id() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen::<u64>()
}
