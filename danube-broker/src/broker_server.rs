use crate::broker_service::BrokerService;
use crate::producer::{self, Producer};
use crate::proto::stream_server::{Stream, StreamServer};
use crate::proto::{
    ConsumerRequest, ConsumerResponse, MessageRequest, MessageResponse, ProducerRequest,
    ProducerResponse,
};
use crate::subscription::SubscriptionOptions;
use crate::topic::Topic;

//use prost::Message;
use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetails, FieldViolation, StatusExt};
use tracing::info;

#[derive(Debug)]
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
            .add_service(StreamServer::new(self))
            .serve(socket_addr)
            .await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl Stream for DanubeServerImpl {
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

        if !validate_topic(&req.topic_name) {
            //TODO don't have yet a defined format for topic name
            err_details.add_bad_request_violation("topic", "the topic should contain only alphanumerics and _ , and the size between 5 to 20 characters");
            let status =
                Status::with_error_details(Code::InvalidArgument, "bad request", err_details);
            return Err(status);
        }

        let topic_ref: &mut Topic;

        {
            let mut service = self.service.lock().await;
            match service.get_topic(req.topic_name.clone(), true) {
                Ok(top) => topic_ref = top,
                Err(err) => {
                    err_details.set_resource_info(
                        "topic",
                        "topic",
                        req.producer_name.clone(),
                        &err.to_string(),
                    );
                    let status = Status::with_error_details(
                        Code::NotFound,
                        "unable to create topic",
                        err_details,
                    );
                    return Err(status);
                }
            }

            if let Some(schema_req) = req.schema {
                //TODO! save Schema to metadata Store, use from resources the topic to get and put schema
                topic_ref.add_schema(schema_req);
            }
        }

        //TODO! Here insert the auth/authz, check if it is authorized to perform the Topic Operation, add a producer

        let mut service = self.service.lock().await;
        if service.check_if_producer_exist(req.topic_name.clone(), req.producer_name.clone()) {
            err_details.add_precondition_failure_violation(
                "ptoducer_id",
                "already present",
                "the producer is already present on the connection",
            );
            let status =
                Status::with_error_details(Code::AlreadyExists, "bad request", err_details);
            return Err(status);
        }

        // for faster boostrap, or in certain conditions we may want to create an initial subscription
        // same time with the producer creation. But not doing this for now, leave to consumers to create subscription.

        let new_producer = if let Ok(prod) = service.create_new_producer(
            req.producer_name.clone(),
            req.topic_name,
            // req.schema,
            req.producer_access_mode,
        ) {
            prod
        } else {
            let status = Status::with_error_details(
                Code::Unknown,
                "unable to create the producer",
                err_details,
            );
            return Err(status);
        };

        let response = ProducerResponse {
            request_id: req.request_id,
            producer_name: req.producer_name,
            producer_id: new_producer.get_id(),
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
                err_details.add_bad_request_violation(
                    "producer",
                    "the producer with id {req.producer_id} does not exist",
                );
                let status =
                    Status::with_error_details(Code::InvalidArgument, "bad request", err_details);
                return Err(status);
            }
            Entry::Occupied(_) => (),
        };

        let topic = match service.get_topic_for_producer(req.producer_id) {
            Ok(topic) => topic,
            Err(err) => {
                err_details.add_bad_request_violation("topic", err.to_string());
                let status =
                    Status::with_error_details(Code::InvalidArgument, "bad request", err_details);
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
                err_details.add_bad_request_violation("producer", err.to_string());
                let status = Status::with_error_details(
                    Code::Internal,
                    "unable to publish the message",
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
        if service.check_if_consumer_exist(&req.consumer_name, &req.subscription, &req.topic_name) {
            err_details.add_precondition_failure_violation(
                "consumer",
                "already present",
                "the consumer is already associated to subscription",
            );
            let status =
                Status::with_error_details(Code::AlreadyExists, "bad request", err_details);
            return Err(status);
        }

        // check if the topic policies allow the creation of the subscription if it doesn't exist
        if !service.allow_subscription_creation(&req.topic_name) {
            err_details.set_bad_request(vec![FieldViolation::new("field_1", "description 1")]);
            let status = Status::with_error_details(
                Code::PermissionDenied,
                "not allowed to create subscription on the topic",
                err_details,
            );
            return Err(status);
        }

        let consumer_id = 1; //TODO! should be generated somehow

        let subscription_options = SubscriptionOptions {
            subscription_name: req.subscription,
            subscription_type: req.subscription_type,
            consumer_id: consumer_id as f32,
            consumer_name: req.consumer_name,
        };

        service.subscribe(&req.topic_name, subscription_options);

        todo!()
    }
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
