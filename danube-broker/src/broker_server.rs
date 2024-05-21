use crate::broker_service::BrokerService;
use crate::proto::danube_server::{Danube, DanubeServer};
use crate::proto::{ConsumerRequest, ConsumerResponse, ProducerRequest, ProducerResponse};
use crate::topic::Topic;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetails, StatusExt};
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
            .add_service(DanubeServer::new(self))
            .serve(socket_addr)
            .await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl Danube for DanubeServerImpl {
    // CMD to create a new Producer
    async fn create_producer(
        &self,
        request: Request<ProducerRequest>,
    ) -> Result<Response<ProducerResponse>, tonic::Status> {
        let req = request.into_inner();
        let mut service = self.service.lock().unwrap();

        info!(
            "{} {} {} {}",
            req.request_id, req.producer_id, req.producer_name, req.topic,
        );

        let mut err_details = ErrorDetails::new();

        if validate_topic(&req.topic) == false {
            //TODO don't have yet a defined format for topic name
            err_details.add_bad_request_violation("topic", "the topic should contain only alphanumerics and _ , and the size between 5 to 20 characters");
        }

        //TODO Here insert the auth/authz, check if it is authorized to perform the Topic Operation, add a producer

        if !service.check_if_producer_exist(req.producer_id) {
            err_details.add_precondition_failure_violation(
                "ptoducer_id",
                "already present",
                "the producer is already present on the connection",
            );
        }

        let topic_ref: &mut Topic;

        match service.get_topic(req.topic.clone(), true) {
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

        // for faster boostrap, or in certain conditions we may want to create an initial subscription
        // same time with the producer creation. But not doing this for now, leave to consumers to create subscription.

        service.build_new_producer(
            req.producer_id,
            req.producer_name,
            req.topic,
            // req.schema,
            req.producer_access_mode,
        );
        //handle error !!!

        if err_details.has_bad_request_violations()
            || err_details.has_precondition_failure_violations()
        {
            let status =
                Status::with_error_details(Code::InvalidArgument, "bad request", err_details);
            return Err(status);
        }

        let response = ProducerResponse {
            request_id: req.request_id,
        };

        Ok(tonic::Response::new(response))
    }

    // CMD to create a new Consumer
    async fn subscribe(
        &self,
        _request: Request<ConsumerRequest>,
    ) -> Result<Response<ConsumerResponse>, tonic::Status> {
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
