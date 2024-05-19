use crate::proto::danube_server::Danube;
use crate::proto::{ConsumerRequest, ConsumerResponse, ProducerRequest, ProducerResponse};

use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetails, StatusExt};

use tracing::info;

#[derive(Debug, Default)]
pub(crate) struct DanubeServerImpl {}

#[tonic::async_trait]
impl Danube for DanubeServerImpl {
    // CMD to create a new Producer
    async fn create_producer(
        &self,
        request: Request<ProducerRequest>,
    ) -> Result<Response<ProducerResponse>, tonic::Status> {
        let req = request.into_inner();

        let mut err_details = ErrorDetails::new();

        if validate_topic(&req.topic) == false {
            //TODO don't have yet a defined format for topic name
            err_details.add_bad_request_violation("topic", "the topic should contain only alphanumerics and _ , and the size between 5 to 20 characters");
        }

        //TODO Here insert the auth/authz, check if it is authorized to perform the Topic Operation, add a producer

        info!(
            "{} {} {} {}",
            req.request_id, req.producer_id, req.producer_name, req.topic,
        );

        if err_details.has_bad_request_violations() {
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
