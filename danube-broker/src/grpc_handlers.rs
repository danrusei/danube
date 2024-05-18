use crate::proto::danube_server::Danube;
use crate::proto::{ConsumerRequest, ConsumerResponse, ProducerRequest, ProducerResponse};

use tonic::{Request, Response};

#[derive(Debug, Default)]
pub(crate) struct DanubeServerImpl {}

#[tonic::async_trait]
impl Danube for DanubeServerImpl {
    async fn create_producer(
        &self,
        request: Request<ProducerRequest>,
    ) -> Result<Response<ProducerResponse>, tonic::Status> {
        let req = request.get_ref();

        if validate_topic(req.topic.as_ref()) == false {
            // respond with invalid topic name
        }

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
