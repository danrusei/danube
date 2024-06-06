use crate::{
    errors::{DanubeError, Result},
    DanubeClient,
};

use crate::proto::{
    consumer_service_client::ConsumerServiceClient, ConsumerRequest, ConsumerResponse,
    ReceiveRequest, StreamMessage,
};

use futures_core::Stream;
use futures_util::stream::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tonic_types::pb::{bad_request, BadRequest};
use tonic_types::StatusExt;

/// Represents a Consumer
#[derive(Debug)]
pub struct Consumer {
    // the Danube client
    client: DanubeClient,
    // the topic name, from where the messages are consumed
    topic_name: String,
    // the name of the Consumer
    consumer_name: String,
    // unique identifier of the consumer, provided by the Broker
    consumer_id: Option<u64>,
    // the name of the subscription the consumer is attached to
    subscription: String,
    // the type of the subscription, that can be Shared and Exclusive
    subscription_type: SubType,
    // other configurable options for the consumer
    consumer_options: ConsumerOptions,
    // unique identifier for every request sent by consumer
    request_id: AtomicU64,
    // the grpc client cnx
    stream_client: Option<ConsumerServiceClient<tonic::transport::Channel>>,
}

#[derive(Debug, Clone)]
pub enum SubType {
    Shared,    // Multiple consumers can subscribe to the topic concurrently.
    Exclusive, // Only one consumer can subscribe to the topic at a time.
}

impl Consumer {
    pub fn new(
        client: DanubeClient,
        topic_name: String,
        consumer_name: String,
        subscription: String,
        sub_type: Option<SubType>,
        consumer_options: ConsumerOptions,
    ) -> Self {
        let subscription_type = if let Some(sub_type) = sub_type {
            sub_type
        } else {
            SubType::Shared
        };

        Consumer {
            client,
            topic_name,
            consumer_name,
            consumer_id: None,
            subscription,
            subscription_type,
            consumer_options,
            request_id: AtomicU64::new(0),
            stream_client: None,
        }
    }
    pub async fn subscribe(&mut self) -> Result<u64> {
        // Initialize the gRPC client connection
        self.connect().await?;

        let req = ConsumerRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            topic_name: self.topic_name.clone(),
            consumer_name: self.consumer_name.clone(),
            subscription: self.subscription.clone(),
            subscription_type: self.subscription_type.clone() as i32,
        };

        let request = tonic::Request::new(req);

        let mut client = self.stream_client.as_mut().unwrap();
        let response: std::result::Result<Response<ConsumerResponse>, Status> =
            client.subscribe(request).await;

        match response {
            Ok(resp) => {
                let r = resp.into_inner();
                self.consumer_id = Some(r.consumer_id);
                return Ok(r.consumer_id);
            }
            // maybe some checks on the status, if anything can be handled by server
            Err(status) => return Err(DanubeError::FromStatus(status)),
        };
    }

    // receive messages
    pub async fn receive(
        &mut self,
    ) -> Result<impl Stream<Item = std::result::Result<StreamMessage, Status>>> {
        let receive_request = ReceiveRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            consumer_id: self.consumer_id.unwrap(),
        };

        let mut stream_client = self.stream_client.as_mut().unwrap();

        let response = stream_client.receive_messages(receive_request).await?;
        Ok(response.into_inner())
    }

    async fn connect(&mut self) -> Result<()> {
        let grpc_cnx = self
            .client
            .cnx_manager
            .get_connection(&self.client.uri, &self.client.uri)
            .await?;
        let client = ConsumerServiceClient::new(grpc_cnx.grpc_cnx.clone());
        self.stream_client = Some(client);
        Ok(())
    }
}

pub struct ConsumerBuilder {
    client: DanubeClient,
    topic: Option<String>,
    consumer_name: Option<String>,
    subscription: Option<String>,
    subscription_type: Option<SubType>,
    consumer_options: ConsumerOptions,
}

impl ConsumerBuilder {
    pub fn new(client: &DanubeClient) -> Self {
        ConsumerBuilder {
            client: client.clone(),
            topic: None,
            consumer_name: None,
            subscription: None,
            subscription_type: None,
            consumer_options: ConsumerOptions::default(),
        }
    }
    /// sets the consumer's topic
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// sets the consumer's name
    pub fn with_consumer_name(mut self, consumer_name: impl Into<String>) -> Self {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    /// attach to a subscription
    pub fn with_subscription(mut self, subscription_name: impl Into<String>) -> Self {
        self.subscription = Some(subscription_name.into());
        self
    }

    pub fn with_subscription_type(mut self, subscription_type: SubType) -> Self {
        self.subscription_type = Some(subscription_type);
        self
    }
    pub fn build(self) -> Consumer {
        let topic = self.topic.expect("you should specify the topic");
        let consumer_name = self
            .consumer_name
            .expect("you should provide a name for the consumer");
        let subscription = self
            .subscription
            .expect("you should provide the name of the subscription");
        Consumer::new(
            self.client,
            topic,
            consumer_name,
            subscription,
            self.subscription_type,
            self.consumer_options,
        )
    }
}

/// Configuration options for consumers
#[derive(Debug, Clone, Default)]
pub struct ConsumerOptions {
    // schema used to encode the messages
    pub others: String,
}
