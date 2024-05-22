use crate::errors::DanubeError;
use crate::proto::{
    danube_client, MessageRequest, MessageResponse, ProducerAccessMode, ProducerRequest,
    ProducerResponse,
};
use crate::{errors::Result, DanubeClient};
use crate::{
    message::{MessageMetadata, SendMessage},
    schema::{Schema, SchemaType},
};

use bytes::Bytes;
use tonic::{Response, Status};
use tonic_types::pb::{bad_request, BadRequest};
use tonic_types::StatusExt;

pub struct Producer {
    client: DanubeClient,
    topic: String,
    name: String,
    schema: Option<Schema>,
    producer_options: ProducerOptions,
}

impl Producer {
    pub fn new(
        client: DanubeClient,
        topic: String,
        name: String,
        schema: Option<Schema>,
        producer_options: ProducerOptions,
    ) -> Self {
        Producer {
            client,
            topic,
            name,
            schema,
            producer_options,
        }
    }
    pub async fn create(&self) -> Result<()> {
        // default schema is Bytes if not specified
        let mut schema = Schema::new("bytes_schema".into(), SchemaType::Bytes);

        if let Some(sch) = self.schema.clone() {
            schema = sch;
        }

        let req = ProducerRequest {
            request_id: 1,
            producer_name: self.name.clone(),
            topic_name: self.topic.clone(),
            schema: Some(schema.to_proto()),
            producer_access_mode: ProducerAccessMode::Shared.into(),
        };

        let request = tonic::Request::new(req);

        let grpc_cnx = self
            .client
            .cnx_manager
            .get_connection(&self.client.uri, &self.client.uri)
            .await?;

        let mut client = danube_client::DanubeClient::new(grpc_cnx.grpc_cnx.clone());
        let response: std::result::Result<Response<ProducerResponse>, Status> =
            client.create_producer(request).await;

        match response {
            Ok(resp) => {
                let r = resp.into_inner();
                println!(
                    "Response: req_id: {:?}, producer_id: {:?}",
                    r.request_id, r.producer_id
                );
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

        Ok(())
    }

    // the Producer sends messages to the topic
    pub async fn send(&self, data: impl Into<Bytes>) -> Result<()> {
        let meta_data = MessageMetadata {
            producer_name: self.name.clone(),
            sequence_id: todo!(),
            publish_time: todo!(),
        };

        let send_message = SendMessage {
            request_id: todo!(),
            producer_id: todo!(),
            metadata: Some(meta_data),
            message: todo!(),
        };

        let req: MessageRequest = send_message.to_proto();

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ProducerBuilder {
    client: DanubeClient,
    topic: Option<String>,
    name: Option<String>,
    schema: Option<Schema>,
    producer_options: ProducerOptions,
}

impl ProducerBuilder {
    pub fn new(client: &DanubeClient) -> Self {
        ProducerBuilder {
            client: client.clone(),
            topic: None,
            name: None,
            schema: None,
            producer_options: ProducerOptions::default(),
        }
    }

    /// sets the producer's topic
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// sets the producer's name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_schema(mut self, schema_name: String, schema_type: SchemaType) -> Self {
        self.schema = Some(Schema::new(schema_name, schema_type));
        self
    }

    pub fn with_options(mut self, options: ProducerOptions) -> Self {
        self.producer_options = options;
        self
    }

    pub fn build(self) -> Producer {
        Producer {
            client: self.client,
            topic: self
                .topic
                .expect("can't create a producer without assigning to a topic"),
            name: self
                .name
                .expect("you should provide a name to the created producer"),
            schema: self.schema,
            producer_options: self.producer_options,
        }
    }
}

/// Configuration options for producers
#[derive(Debug, Clone, Default)]
pub struct ProducerOptions {
    // schema used to encode the messages
    pub others: String,
}
