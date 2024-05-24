use crate::{errors::Result, DanubeClient};

/// Represents a Consumer
pub struct Consumer {
    // the Danube client
    client: DanubeClient,
    // the topic name, from where the messages are consumed
    topic: String,
    // the name of the Consumer
    consumer_name: String,
    // the name of the subscription the consumer is attached to
    subscription: String,
    // the type of the subscription, that can be Shared and Exclusive
    subscription_type: SubType,
    // other configurable options for the consumer
    consumer_options: ConsumerOptions,
}

pub enum SubType {
    Shared,    // Multiple consumers can subscribe to the topic concurrently.
    Exclusive, // Only one consumer can subscribe to the topic at a time.
}

impl Consumer {
    pub fn new(
        client: DanubeClient,
        topic: String,
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
            topic,
            consumer_name,
            subscription,
            subscription_type,
            consumer_options,
        }
    }
    pub async fn subscribe(&self) -> Result<()> {
        todo!()
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
