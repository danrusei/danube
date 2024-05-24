use crate::{errors::Result, DanubeClient};

use crate::proto::{stream_client::StreamClient, ConsumerRequest, ConsumerResponse};

use std::sync::atomic::{AtomicU64, Ordering};
use tonic::{Response, Status};
use tonic_types::pb::{bad_request, BadRequest};
use tonic_types::StatusExt;

/// Represents a Consumer
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
    stream_client: Option<StreamClient<tonic::transport::Channel>>,
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
    pub async fn subscribe(&mut self) -> Result<()> {
        // Initialize the gRPC client connection
        self.initialize_grpc_client().await?;

        let req = ConsumerRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            topic_name: self.topic_name.clone(),
            consumer_name: self.consumer_name.clone(),
            subscription: self.subscription.clone(),
            subscription_type: self.subscription_type.clone() as i32,
        };

        let request = tonic::Request::new(req);

        let mut client = self.stream_client.as_mut().unwrap().clone();
        let response: std::result::Result<Response<ConsumerResponse>, Status> =
            client.subscribe(request).await;

        match response {
            Ok(resp) => {
                let r = resp.into_inner();
                self.consumer_id = Some(r.consumer_id);
                println!(
                    "Response: req_id: {:?}, producer_id: {:?}",
                    r.request_id, r.consumer_id
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
    async fn initialize_grpc_client(&mut self) -> Result<()> {
        let grpc_cnx = self
            .client
            .cnx_manager
            .get_connection(&self.client.uri, &self.client.uri)
            .await?;
        let client = StreamClient::new(grpc_cnx.grpc_cnx.clone());
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

/// Configuration options for producers
#[derive(Debug, Clone, Default)]
pub struct ConsumerOptions {
    // schema used to encode the messages
    pub others: String,
}
