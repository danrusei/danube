use anyhow::Result;
use dashmap::DashMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

use crate::proto::danube_server::{Danube, DanubeServer};
use crate::topic::Topic;
use crate::{consumer::Consumer, producer::Producer};

#[derive(Debug)]
pub(crate) struct BrokerService {
    //broker_addr: SocketAddr,
    // a map with namespace wise topics - Namespace --> topicName --> topic
    topics: DashMap<String, DashMap<String, Topic>>,
    // brokers to register listeners for configuration changes or updates
    // config_listeners: DashMap<String, Consumer>,
    // the list of active producers
    producers: HashMap<u64, Producer>,
    // the list of active consumers
    consumers: HashMap<u64, Consumer>,
}

impl BrokerService {
    pub(crate) fn new() -> Self {
        BrokerService {
            //broker_addr: broker_addr,
            topics: DashMap::new(),
            //config_listeners: DashMap::new,
            producers: HashMap::new(),
            consumers: HashMap::new(),
        }
    }

    pub(crate) async fn get_topic(
        &mut self,
        topic: String,
        create_if_missing: bool,
    ) -> Result<Topic> {
        todo!()
    }

    pub(crate) async fn delete_topic(&mut self, topic: String) -> Result<Topic> {
        todo!()
    }

    pub(crate) fn check_if_producer_exist(&self, producer_id: u64) -> bool {
        self.producers.contains_key(&producer_id)
    }

    // pub(crate) async fn register_configuration_listener(
    //     &mut self,
    //     config_key: String,
    //     listener: Consumer,
    // ) {
    //     self.config_listeners.insert(config_key, listener);
    // }
}
