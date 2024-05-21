use crate::errors::DanubeError;
use crate::proto::{danube_client, ProducerAccessMode, ProducerRequest, ProducerResponse, Schema};
use crate::{errors::Result, DanubeClient};

use tonic::{Response, Status};
use tonic_types::pb::{bad_request, BadRequest};
use tonic_types::StatusExt;

pub struct Producer {
    danube: DanubeClient,
    topic: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProducerBuilder {
    danube: DanubeClient,
    topic: Option<String>,
    name: Option<String>,
}

impl ProducerBuilder {
    pub fn new(client: &DanubeClient) -> Self {
        ProducerBuilder {
            danube: client.clone(),
            topic: None,
            name: None,
        }
    }

    /// sets the producer's topic
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// sets the producer's name
    pub fn with_name<S: Into<String>>(mut self, name: S) -> Self {
        self.name = Some(name.into());
        self
    }

    pub async fn create(self) -> Result<Producer> {
        let req = ProducerRequest {
            request_id: 1,
            producer_name: "hello_producer".to_string(),
            topic_name: "hello_topic".to_string(),
            schema: Some(Schema {
                name: "schema_name".to_string(),
                schema_data: "1".as_bytes().to_vec(),
                type_schema: 0,
            }),
            producer_access_mode: ProducerAccessMode::Shared.into(),
        };

        let request = tonic::Request::new(req);

        let grpc_cnx = self
            .danube
            .cnx_manager
            .get_connection(&self.danube.uri, &self.danube.uri)
            .await?;

        let mut client = danube_client::DanubeClient::new(grpc_cnx.grpc_cnx.clone());
        let response: std::result::Result<Response<ProducerResponse>, Status> =
            client.create_producer(request).await;

        match response {
            Ok(resp) => {
                let r = resp.into_inner();
                println!("Response: req_id {:?} {:?}", r.request_id, r.producer_id);
            }
            Err(status) => {
                //let err_details = status.get_error_details();
                match status.get_error_details() {
                    error_details => {
                        println!("Invalid request: {:?}", error_details)
                    }
                }
            }
        };

        Ok(Producer {
            danube: self.danube,
            topic: self.topic,
            name: self.name,
        })
    }
}
