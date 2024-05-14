use dashmap::DashMap;
use std::net::SocketAddr;

use crate::proto::danube_server::{Danube, DanubeServer};
use crate::proto::{ConsumerRequest, ConsumerResponse, ProducerRequest, ProducerResponse};
use crate::services::topic::Topic;
use tonic::transport::Server;
use tonic::{Request, Response};

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
            .add_service(DanubeServer::new(self))
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

#[tonic::async_trait]
impl Danube for BrokerService {
    async fn create_producer(
        &self,
        request: Request<ProducerRequest>,
    ) -> Result<Response<ProducerResponse>, tonic::Status> {
        let req = request.get_ref();

        println!(
            "{} {} {} {}",
            req.request_id, req.producer_id, req.producer_name, req.topic,
        );

        let response = ProducerResponse {
            request_id: req.request_id,
        };

        Ok(tonic::Response::new(response))
    }
    async fn subscribe(
        &self,
        _request: Request<ConsumerRequest>,
    ) -> Result<Response<ConsumerResponse>, tonic::Status> {
        todo!()
    }
}
