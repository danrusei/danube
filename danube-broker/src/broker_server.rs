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

        match service.get_topic(&req.topic_name, req.schema, true).await {
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
            let status =
                Status::already_exists("This producer is already present on the connection");
            return Err(status);
        }

        let new_producer_id = match service.create_new_producer(
            &req.producer_name,
            &req.topic_name,
            req.producer_access_mode,
        ) {
            Ok(prod_id) => prod_id,
            Err(err) => {
                let status = Status::permission_denied(format!(
                    "Not able to create the Producer: {}",
                    err.to_string(),
                ));
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

        // check if the producer exist
        match service.producers.entry(req.producer_id) {
            Entry::Vacant(_) => {
                let status = Status::not_found(format!(
                    "The producer with id {} does not exist",
                    req.producer_id
                ));
                return Err(status);
            }
            Entry::Occupied(_) => (),
        };

        let topic = match service.get_topic_for_producer(req.producer_id) {
            Ok(topic) => topic,
            Err(err) => {
                // Should not happen, as the Producer can only be created if it's associated with the Topic
                let status = Status::internal(format!(
                    "Unable to get the topic for the producer: {}",
                    err.to_string()
                ));
                return Err(status);
            }
        };

        //TODO! should not be an Option, as it is mandatory to be present in the message request
        let meta = req.metadata.unwrap();

        match topic
            .publish_message(req.producer_id, meta.sequence_id, req.message)
            .await
        {
            Ok(_) => (),
            Err(err) => {
                let status = Status::permission_denied(format!(
                    "Unable to publish the message: {}",
                    err.to_string()
                ));
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

        match service.get_topic(&req.topic_name, None, false).await {
            Ok(topic_name) => {
                trace!("topic_name: {} was found ", topic_name)
            }
            Err(err) => {
                let status = Status::invalid_argument(err.to_string());
                return Err(status);
            }
        }

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
            None => {
                // if the consumer doesn't exist it attempts to create below
            }
        }

        // check if the topic policies allow the creation of the subscription
        if !service.allow_subscription_creation(&req.topic_name) {
            let status = Status::permission_denied(format!(
                "Not allowed to create the subscription for the topic: {}",
                &req.topic_name
            ));

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
                let status = Status::permission_denied(format!(
                    "Not able to subscribe to the topic {} due to {}",
                    &req.topic_name,
                    err.to_string()
                ));
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
            let status = Status::not_found(format!(
                "The consumer with the id {} does not exist",
                consumer_id
            ));
            return Err(status);
        };

        // sends the channel's tx to consumer
        consumer.lock().await.set_tx(tx_consumer);

        //for each consumer spawn a task to send messages
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
