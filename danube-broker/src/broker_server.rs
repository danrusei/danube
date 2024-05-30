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
use bytes::Bytes;
use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetails, FieldViolation, StatusExt};
use tracing::{info, trace, Level};

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

        info!("Server is listening on address: {socket_addr}");

        Ok(())
    }
}

#[tonic::async_trait]
impl ProducerService for DanubeServerImpl {
    // CMD to create a new Producer
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_producer(
        &self,
        request: Request<ProducerRequest>,
    ) -> Result<Response<ProducerResponse>, tonic::Status> {
        let req = request.into_inner();

        info!(
            "New Producer request with name: {} for topic: {}",
            req.producer_name, req.topic_name,
        );

        let mut err_details = ErrorDetails::new();

        let mut service = self.service.lock().await;

        match service.find_or_create_topic(&req.topic_name, req.schema, true) {
            Ok(topic_name) => {
                trace!(
                    "topic_name: {} was found or was successfully created",
                    topic_name
                )
            }
            Err(err) => {
                let status = Status::invalid_argument(err.to_string());
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

        info!(
            "The Producer with name: {} and with id: {}, has been created",
            req.producer_name, new_producer_id
        );

        let response = ProducerResponse {
            request_id: req.request_id,
            producer_name: req.producer_name,
            producer_id: new_producer_id,
        };

        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn send_message(
        &self,
        request: Request<MessageRequest>,
    ) -> Result<Response<MessageResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!(
            "New message {} from producer {} with metadata {:?} was received",
            req.request_id,
            req.producer_id,
            req.metadata
        );

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
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn subscribe(
        &self,
        request: Request<ConsumerRequest>,
    ) -> Result<Response<ConsumerResponse>, tonic::Status> {
        let req = request.into_inner();

        info!(
            "New Consumer request with name: {} for topic: {} with subscription_type {}",
            req.consumer_name, req.topic_name, req.subscription_type
        );

        let mut err_details = ErrorDetails::new();

        // TODO! check if the subscription is authorized to consume from the topic (isTopicOperationAllowed)

        let mut service = self.service.lock().await;

        match service
            .check_if_consumer_exist(&req.consumer_name, &req.subscription, &req.topic_name)
            .await
        {
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

        let sub_name = subscription_options.subscription_name.clone();

        let consumer_id = match service
            .subscribe(&req.topic_name, subscription_options)
            .await
        {
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

        info!(
            "The Consumer with id: {} for subscription: {}, has been created.",
            consumer_id, sub_name
        );

        let response = ConsumerResponse {
            request_id: req.request_id,
            consumer_id: consumer_id,
            consumer_name: req.consumer_name,
        };

        Ok(tonic::Response::new(response))
    }

    // Stream of messages to Consumer
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn receive_messages(
        &self,
        request: tonic::Request<ReceiveRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReceiveMessagesStream>, tonic::Status> {
        let (tx, rx) = mpsc::channel(4); // Buffer size of 4, adjust as needed
        let (tx_consumer, mut rx_consumer) = mpsc::channel(4);
        let consumer_id = request.into_inner().consumer_id;

        info!(
            "The Consumer with id: {} requested to receive messages",
            consumer_id
        );

        let mut err_details = ErrorDetails::new();

        let mut service = self.service.lock().await;

        let consumer = if let Some(consumer) = service.get_consumer(consumer_id) {
            consumer
        } else {
            err_details.set_bad_request(vec![FieldViolation::new(
                "Consumer",
                "Consumer with ID can't be found",
            )]);
            let status = Status::with_error_details(
                Code::NotFound,
                "the provided consumer ID does not exist",
                err_details,
            );
            return Err(status);
        };

        consumer.lock().await.set_tx(tx_consumer);

        ///  call the internal function to trigger the send_messages
        ///  how to send the tx to Consumer ???????
        ///
        tokio::spawn(async move {
            loop {
                if let Some(messages) = rx_consumer.recv().await {
                    let request_id = 1;

                    let stream_messages = StreamMessage {
                        request_id: request_id,
                        messages: messages,
                    };

                    if tx.send(Ok(stream_messages)).await.is_err() {
                        break;
                    }
                    trace!(
                        "The message with request_id: {} was sent to consumer",
                        request_id
                    );
                }

                //tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // Consumer acknowledge the received message
    async fn ack(
        &self,
        request: tonic::Request<AckRequest>,
    ) -> std::result::Result<tonic::Response<AckResponse>, tonic::Status> {
        todo!()
    }
}
