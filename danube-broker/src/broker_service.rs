use dashmap::DashMap;
use std::net::SocketAddr;
use tonic::transport::Server;

use crate::grpc_handlers::DanubeServerImpl;
use crate::proto::danube_server::{Danube, DanubeServer};
use crate::topic::Topic;

pub(crate) struct BrokerService {
    broker_addr: SocketAddr,
    // a map with namespace wise topics - Namespace --> topicName --> topic
    topics: DashMap<String, DashMap<String, Topic>>,
    // brokers to register listeners for configuration changes or updates
    // config_listeners: DashMap<String, Consumer>,
}

impl BrokerService {
    pub(crate) fn new(broker_addr: SocketAddr) -> Self {
        BrokerService {
            broker_addr: broker_addr,
            topics: DashMap::new(),
            //config_listeners: DashMap::new,
        }
    }
    pub(crate) async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        //TODO! start other backgroud services like PublishRateLimiter, DispatchRateLimiter,
        // compaction, innactivity monitor

        let socket_addr = self.broker_addr.clone();

        Server::builder()
            .add_service(DanubeServer::new(DanubeServerImpl::default()))
            .serve(socket_addr)
            .await?;

        Ok(())
    }
    pub(crate) async fn get_topic(
        &mut self,
        topic: String,
        create_if_missing: bool,
    ) -> Result<Topic, Box<dyn std::error::Error>> {
        todo!()
    }

    pub(crate) async fn delete_topic(
        &mut self,
        topic: String,
    ) -> Result<Topic, Box<dyn std::error::Error>> {
        todo!()
    }

    // pub(crate) async fn register_configuration_listener(
    //     &mut self,
    //     config_key: String,
    //     listener: Consumer,
    // ) {
    //     self.config_listeners.insert(config_key, listener);
    // }
}
