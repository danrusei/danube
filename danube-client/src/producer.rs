use crate::proto::{
    producer_service_client::ProducerServiceClient, MessageRequest, MessageResponse,
    ProducerAccessMode, ProducerRequest, ProducerResponse,
};
use crate::{
    errors::{decode_error_details, DanubeError, Result},
    message::{MessageMetadata, SendMessage},
    schema::{Schema, SchemaType},
    DanubeClient,
};

use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};
use tonic::{transport::Uri, Response, Status};
use tonic_types::pb::{bad_request, BadRequest};
use tonic_types::StatusExt;
use tracing::{info, warn};

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
        self.connect(&self.client.uri.clone()).await?;

        // default schema is Bytes if not specified
        let mut schema = Schema::new("bytes_schema".into(), SchemaType::Bytes);

        if let Some(sch) = self.schema.clone() {
            schema = sch;
        }

        let req = ProducerRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            producer_name: self.producer_name.clone(),
            topic_name: self.topic.clone(),
            schema: Some(schema.into()),
            producer_access_mode: ProducerAccessMode::Shared.into(),
        };

        let max_retries = 4;
        let mut attempts = 0;

        let mut broker_addr = self.client.uri.clone();

        // The loop construct continues to try the create_producer call
        // until it either succeeds in less max retries or fails with a different error.
        loop {
            let request = tonic::Request::new(req.clone());

            let mut client = self.stream_client.as_mut().unwrap().clone();
            let response: std::result::Result<Response<ProducerResponse>, Status> =
                client.create_producer(request).await;

            match response {
                Ok(resp) => {
                    let response = resp.into_inner();
                    self.producer_id = Some(response.producer_id);
                    return Ok(response.producer_id);
                }
                Err(status) => {
                    let error_message = decode_error_details(&status);

                    attempts += 1;
                    if attempts >= max_retries {
                        return Err(DanubeError::FromStatus(status, error_message));
                    }

                    // if not a SERVICE_NOT_READY error received from broker returns
                    // else continue to loop as the topic may be in process to be assigned to a broker
                    if let Some(error_m) = &error_message {
                        if error_m.error_type != 3 {
                            return Err(DanubeError::FromStatus(status, error_message));
                        }
                    }

                    // as we are in SERVICE_NOT_READY case, let give some space to the broker to assign the topic
                    sleep(Duration::from_secs(2)).await;

                    match self
                        .client
                        .lookup_service
                        .handle_lookup(&broker_addr, &self.topic)
                        .await
                    {
                        Ok(addr) => {
                            dbg!(&addr);
                            broker_addr = addr.clone();
                            self.connect(&addr).await?;
                        }

                        Err(err) => {
                            if let Some(status) = err.extract_status() {
                                if let Some(error_message) = decode_error_details(status) {
                                    if error_message.error_type != 3 {
                                        return Err(DanubeError::FromStatus(
                                            status.to_owned(),
                                            Some(error_message),
                                        ));
                                    }
                                }
                            } else {
                                warn!("Lookup request failed with error:  {}", err);
                                return Err(DanubeError::Unrecoverable(format!(
                                    "Lookup failed with error: {}",
                                    err
                                )));
                            }
                        }
                    }
                }
            };
        }
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
            Err(status) => {
                let decoded_message = decode_error_details(&status);
                return Err(DanubeError::FromStatus(status, decoded_message));
            }
        }
    }
    async fn connect(&mut self, addr: &Uri) -> Result<()> {
        let grpc_cnx = self.client.cnx_manager.get_connection(addr, addr).await?;
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
