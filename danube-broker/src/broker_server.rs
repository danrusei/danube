use crate::broker_service::BrokerService;
use crate::producer::{self, Producer};
use crate::proto::{
    consumer_service_server::{ConsumerService, ConsumerServiceServer},
    producer_service_server::{ProducerService, ProducerServiceServer},
};
use crate::proto::{
    AckRequest, AckResponse, ConsumerRequest, ConsumerResponse, MessageRequest, MessageResponse,
    ProducerRequest, ProducerResponse, ReceiveRequest, StreamMessage,
};
use crate::subscription::SubscriptionOptions;
use crate::topic::Topic;

//use prost::Message;
use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetails, FieldViolation, StatusExt};
use tracing::info;

#[derive(Debug, Clone)]
pub(crate) struct DanubeServerImpl {
    service: Arc<Mutex<BrokerService>>,
    broker_addr: SocketAddr,
}

impl DanubeServerImpl {
    pub(crate) fn new(service: Arc<Mutex<BrokerService>>, broker_addr: SocketAddr) -> Self {
        DanubeServerImpl {
            service,
            broker_addr,
        }
    }
    pub(crate) async fn start(self) -> anyhow::Result<()> {
        //TODO! start other backgroud services like PublishRateLimiter, DispatchRateLimiter,
        // compaction, innactivity monitor

        let socket_addr = self.broker_addr.clone();

        Server::builder()
            .add_service(ProducerServiceServer::new(self.clone()))
            .add_service(ConsumerServiceServer::new(self))
            .serve(socket_addr)
            .await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl ProducerService for DanubeServerImpl {
    // CMD to create a new Producer
    async fn create_producer(
        &self,
        request: Request<ProducerRequest>,
    ) -> Result<Response<ProducerResponse>, tonic::Status> {
        let req = request.into_inner();

        info!(
            "{} {} {}",
            req.request_id, req.producer_name, req.topic_name,
        );

        let mut err_details = ErrorDetails::new();

        let mut service = self.service.lock().await;

        match service.find_or_create_topic(&req.topic_name, req.schema, true) {
            Ok(topic_name) => {
                info!("topic_name: {} exist or was created", topic_name)
            }
            Err(err) => {
                err_details.set_bad_request(vec![FieldViolation::new("Topic", err.to_string())]);
                let status = Status::with_error_details(
                    Code::PermissionDenied,
                    "not able to create the Topic",
                    err_details,
                );
                return Err(status);
            }
        }

        //Todo! Here insert the auth/authz, check if it is authorized to perform the Topic Operation, add a producer

        if service.check_if_producer_exist(req.topic_name.clone(), req.producer_name.clone()) {
            err_details.set_bad_request(vec![FieldViolation::new(
                "Producer",
                "This producer is already present on the connection",
            )]);
            let status =
                Status::with_error_details(Code::AlreadyExists, "bad request", err_details);
            return Err(status);
        }

        let new_producer_id = match service.create_new_producer(
            &req.producer_name,
            &req.topic_name,
            req.producer_access_mode,
        ) {
            Ok(prod_id) => prod_id,
            Err(err) => {
                err_details.set_bad_request(vec![FieldViolation::new("Producer", err.to_string())]);
                let status = Status::with_error_details(
                    Code::AlreadyExists,
                    "not able to create the Producer",
                    err_details,
                );
                return Err(status);
            }
        };

        let response = ProducerResponse {
            request_id: req.request_id,
            producer_name: req.producer_name,
            producer_id: new_producer_id,
        };

        Ok(tonic::Response::new(response))
    }

    async fn send_message(
        &self,
        request: Request<MessageRequest>,
    ) -> Result<Response<MessageResponse>, tonic::Status> {
        let req = request.into_inner();

        info!("{} {} {:?}", req.request_id, req.producer_id, req.metadata);

        let mut err_details = ErrorDetails::new();

        let arc_service = self.service.clone();
        let mut service = arc_service.lock().await;

        // check if the producer was created and get topic_name
        match service.producers.entry(req.producer_id) {
            Entry::Vacant(_) => {
                err_details.set_bad_request(vec![FieldViolation::new(
                    "Producer",
                    "the producer with id {req.producer_id} does not exist",
                )]);
                let status = Status::with_error_details(
                    Code::InvalidArgument,
                    "Unable to find the Producer",
                    err_details,
                );
                return Err(status);
            }
            Entry::Occupied(_) => (),
        };

        let topic = match service.get_topic_for_producer(req.producer_id) {
            Ok(topic) => topic,
            Err(err) => {
                err_details.set_bad_request(vec![FieldViolation::new("Topic", err.to_string())]);
                let status = Status::with_error_details(
                    Code::InvalidArgument,
                    "Unable to get the topic for the producer",
                    err_details,
                );
                return Err(status);
            }
        };

        //TODO! this should not be Option, as it is mandatory to be present with the message request
        let meta = req.metadata.unwrap();

        match topic
            .publish_message(req.producer_id, meta.sequence_id, req.message)
            .await
        {
            Ok(_) => (),
            Err(err) => {
                err_details.set_bad_request(vec![FieldViolation::new("Producer", err.to_string())]);
                let status = Status::with_error_details(
                    Code::Internal,
                    "Unable to publish the message",
                    err_details,
                );
                return Err(status);
            }
        };

        let response = MessageResponse {
            request_id: req.request_id,
            sequence_id: meta.sequence_id,
        };

        Ok(tonic::Response::new(response))
    }
}

#[tonic::async_trait]
impl ConsumerService for DanubeServerImpl {
    type ReceiveMessagesStream = ReceiverStream<Result<StreamMessage, Status>>;
    // CMD to create a new Consumer
    async fn subscribe(
        &self,
        request: Request<ConsumerRequest>,
    ) -> Result<Response<ConsumerResponse>, tonic::Status> {
        let req = request.into_inner();

        info!(
            "{} {} {}",
            req.request_id, req.consumer_name, req.topic_name,
        );

        let mut err_details = ErrorDetails::new();

        // TODO! check if the subscription is authorized to consume from the topic (isTopicOperationAllowed)

        let mut service = self.service.lock().await;

        match service.check_if_consumer_exist(
            &req.consumer_name,
            &req.subscription,
            &req.topic_name,
        ) {
            Some(consumer_id) => {
                let response = ConsumerResponse {
                    request_id: req.request_id,
                    consumer_id: consumer_id,
                    consumer_name: req.consumer_name,
                };
                return Ok(tonic::Response::new(response));
            }
            None => {}
        }

        // check if the topic policies allow the creation of the subscription if it doesn't exist
        if !service.allow_subscription_creation(&req.topic_name) {
            err_details.set_bad_request(vec![FieldViolation::new(
                "Subscription",
                "Creation not allowed",
            )]);
            let status = Status::with_error_details(
                Code::PermissionDenied,
                "not allowed to create subscription on the topic",
                err_details,
            );
            return Err(status);
        }

        let subscription_options = SubscriptionOptions {
            subscription_name: req.subscription,
            subscription_type: req.subscription_type,
            consumer_id: None,
            consumer_name: req.consumer_name.clone(),
        };

        let consumer_id = match service.subscribe(&req.topic_name, subscription_options) {
            Ok(id) => id,
            Err(err) => {
                err_details.set_error_info("unable to subscribe", err.to_string(), HashMap::new());
                let status = Status::with_error_details(
                    Code::PermissionDenied,
                    "not abled to subscribe to the topic",
                    err_details,
                );
                return Err(status);
            }
        };

        let response = ConsumerResponse {
            request_id: req.request_id,
            consumer_id: consumer_id,
            consumer_name: req.consumer_name,
        };

        Ok(tonic::Response::new(response))
    }

    // Stream of messages to Consumer
    async fn receive_messages(
        &self,
        request: tonic::Request<ReceiveRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReceiveMessagesStream>, tonic::Status> {
        todo!()
    }

    // Consumer acknowledge the received message
    async fn ack(
        &self,
        request: tonic::Request<AckRequest>,
    ) -> std::result::Result<tonic::Response<AckResponse>, tonic::Status> {
        todo!()
    }
}
