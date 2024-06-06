use crate::proto::{
    producer_service_client::ProducerServiceClient, MessageRequest, MessageResponse,
    ProducerAccessMode, ProducerRequest, ProducerResponse,
};
use crate::{
    errors::{DanubeError, Result},
    message::{MessageMetadata, SendMessage},
    schema::{Schema, SchemaType},
    DanubeClient,
};

use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Response, Status};
use tonic_types::pb::{bad_request, BadRequest};
use tonic_types::StatusExt;

/// Represents a Producer
#[derive(Debug)]
pub struct Producer {
    // the Danube client
    client: DanubeClient,
    // the topic name, used by the producer to publish messages
    topic: String,
    // the name of the producer
    producer_name: String,
    // unique identifier of the producer, provided by the Broker
    producer_id: Option<u64>,
    // unique identifier for every request sent by the producer
    request_id: AtomicU64,
    // it represents the sequence ID of the message within the topic
    message_sequence_id: AtomicU64,
    // the schema represent the message payload schema
    schema: Option<Schema>,
    // other configurable options for the producer
    producer_options: ProducerOptions,
    // the grpc client cnx
    stream_client: Option<ProducerServiceClient<tonic::transport::Channel>>,
}

impl Producer {
    pub fn new(
        client: DanubeClient,
        topic: String,
        producer_name: String,
        schema: Option<Schema>,
        producer_options: ProducerOptions,
    ) -> Self {
        Producer {
            client,
            topic,
            producer_name,
            producer_id: None,
            request_id: AtomicU64::new(0),
            message_sequence_id: AtomicU64::new(0),
            schema,
            producer_options,
            stream_client: None,
        }
    }
    pub async fn create(&mut self) -> Result<u64> {
        // Initialize the gRPC client connection
        self.connect().await?;

        // default schema is Bytes if not specified
        let mut schema = Schema::new("bytes_schema".into(), SchemaType::Bytes);

        if let Some(sch) = self.schema.clone() {
            schema = sch;
        }

        let req = ProducerRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            producer_name: self.producer_name.clone(),
            topic_name: self.topic.clone(),
            schema: Some(schema.to_proto()),
            producer_access_mode: ProducerAccessMode::Shared.into(),
        };

        let request = tonic::Request::new(req);

        let mut client = self.stream_client.as_mut().unwrap().clone();
        let response: std::result::Result<Response<ProducerResponse>, Status> =
            client.create_producer(request).await;

        match response {
            Ok(resp) => {
                let response = resp.into_inner();
                self.producer_id = Some(response.producer_id);
                return Ok(response.producer_id);
            }
            // maybe some checks on the status, if anything can be handled by server
            Err(status) => return Err(DanubeError::FromStatus(status)),
        };
    }

    // the Producer sends messages to the topic
    pub async fn send(&self, data: Vec<u8>) -> Result<u64> {
        let publish_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        let meta_data = MessageMetadata {
            producer_name: self.producer_name.clone(),
            sequence_id: self.message_sequence_id.fetch_add(1, Ordering::SeqCst),
            publish_time: publish_time,
        };

        let send_message = SendMessage {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            producer_id: self
                .producer_id
                .expect("Producer ID should be set before sending messages"),
            metadata: Some(meta_data),
            message: data,
        };

        let req: MessageRequest = send_message.to_proto();

        let mut client = self.stream_client.as_ref().unwrap().clone();
        let response: std::result::Result<Response<MessageResponse>, Status> =
            client.send_message(tonic::Request::new(req)).await;

        match response {
            Ok(resp) => {
                let response = resp.into_inner();
                return Ok(response.sequence_id);
            }
            // maybe some checks on the status, if anything can be handled by server
            Err(status) => return Err(DanubeError::FromStatus(status)),
        }
    }
    async fn connect(&mut self) -> Result<()> {
        let grpc_cnx = self
            .client
            .cnx_manager
            .get_connection(&self.client.uri, &self.client.uri)
            .await?;
        let client = ProducerServiceClient::new(grpc_cnx.grpc_cnx.clone());
        self.stream_client = Some(client);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ProducerBuilder {
    client: DanubeClient,
    topic: Option<String>,
    producer_name: Option<String>,
    schema: Option<Schema>,
    producer_options: ProducerOptions,
}

impl ProducerBuilder {
    pub fn new(client: &DanubeClient) -> Self {
        ProducerBuilder {
            client: client.clone(),
            topic: None,
            producer_name: None,
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
    pub fn with_name(mut self, producer_name: impl Into<String>) -> Self {
        self.producer_name = Some(producer_name.into());
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
        let topic = self
            .topic
            .expect("can't create a producer without assigning to a topic");
        let producer_name = self
            .producer_name
            .expect("you should provide a name to the created producer");
        Producer::new(
            self.client,
            topic,
            producer_name,
            self.schema,
            self.producer_options,
        )
    }
}

/// Configuration options for producers
#[derive(Debug, Clone, Default)]
pub struct ProducerOptions {
    // schema used to encode the messages
    pub others: String,
}
