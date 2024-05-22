use anyhow::{anyhow, Ok, Result};
use dashmap::DashMap;
use rand::Rng;
use std::any;
use std::collections::{hash_map::Entry, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;

use crate::proto::{ProducerAccessMode, Schema};
use crate::topic::{self, Topic};
use crate::{consumer::Consumer, producer::Producer};

#[derive(Debug)]
pub(crate) struct BrokerService {
    //broker_addr: SocketAddr,
    // a map with namespace wise topics - Namespace --> topicName --> topic
    //topics: DashMap<String, DashMap<String, Topic>>,
    topics: HashMap<String, Topic>,
    // brokers to register listeners for configuration changes or updates
    // config_listeners: DashMap<String, Consumer>,
    // the list of active producers
    // producers: HashMap<u64, Producer>,
    // the list of active consumers
    //consumers: HashMap<u64, Consumer>,
}

impl BrokerService {
    pub(crate) fn new() -> Self {
        BrokerService {
            //broker_addr: broker_addr,
            //topics: DashMap::new(),
            //config_listeners: DashMap::new,
            topics: HashMap::new(),
            // producers: HashMap::new(),
            // consumers: HashMap::new(),
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

    // create a new producer and attach to the topic
    pub(crate) fn build_new_producer(
        &mut self,
        producer_name: String,
        topic_name: String,
        producer_access_mode: i32,
    ) -> Result<&Producer> {
        let topic = self.topics.get_mut(&topic_name).expect("todo");
        let producer_id = get_random_id();
        match topic.producers.entry(producer_id) {
            Entry::Vacant(entry) => {
                entry.insert(Producer::new(
                    producer_id,
                    producer_name,
                    topic_name,
                    producer_access_mode,
                ));
            }
            Entry::Occupied(entry) => {
                //let current_producer = entry.get();
                return Err(anyhow!(" the producer already exist"));
            }
        }

        Ok(topic
            .producers
            .get(&producer_id)
            .expect("the producer was created above"))
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
