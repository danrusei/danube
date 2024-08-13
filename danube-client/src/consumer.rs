use crate::{
    errors::{DanubeError, Result},
    proto::StreamMessage,
    topic_consumer::TopicConsumer,
    DanubeClient,
};

use futures::{future::join_all, StreamExt};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

#[derive(Debug, Clone)]
pub enum SubType {
    // Exclusivve - Only one consumer can subscribe to the topic at a time.
    Exclusive,
    // Shared - Multiple consumers can subscribe to the topic concurrently.
    Shared,
    // FailOver - Only one consumer can subscribe to the topic at a time,
    // but waits in standby, if the active consumer disconnect
    FailOver,
}

/// Represents a Consumer
#[derive(Debug)]
pub struct Consumer {
    // the Danube client
    client: DanubeClient,
    // the topic name, from where the messages are consumed
    topic_name: String,
    // the name of the Consumer
    consumer_name: String,
    // the consumers of the topic
    consumers: Vec<Arc<Mutex<TopicConsumer>>>,
    // the name of the subscription the consumer is attached to
    subscription: String,
    // the type of the subscription, that can be Shared and Exclusive
    subscription_type: SubType,
    // other configurable options for the consumer
    consumer_options: ConsumerOptions,
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
            consumers: Vec::new(),
            subscription,
            subscription_type,
            consumer_options,
        }
    }

    pub async fn subscribe(&mut self) -> Result<()> {
        // Get partitions from the topic
        let partitions = self
            .client
            .lookup_service
            .topic_partitions(&self.client.uri, &self.topic_name)
            .await?;

        // Create TopicConsumer for each partition
        let mut tasks = Vec::new();
        for topic_partition in partitions {
            let topic_name = topic_partition.clone();
            let consumer_name = self.consumer_name.clone();
            let subscription = self.subscription.clone();
            let subscription_type = self.subscription_type.clone();
            let consumer_options = self.consumer_options.clone();
            let client = self.client.clone();

            let task = tokio::spawn(async move {
                let mut topic_consumer = TopicConsumer::new(
                    client,
                    topic_name,
                    consumer_name,
                    subscription,
                    Some(subscription_type),
                    consumer_options,
                );
                match topic_consumer.subscribe().await {
                    Ok(_) => Ok(Arc::new(Mutex::new(topic_consumer))),
                    Err(e) => Err(e),
                }
            });

            tasks.push(task);
        }

        // Wait for all tasks to complete
        let results = join_all(tasks).await;

        // Collect results
        let mut topic_consumers = Vec::new();
        for result in results {
            match result {
                Ok(Ok(consumer)) => topic_consumers.push(consumer),
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(DanubeError::Unrecoverable(e.to_string())),
            }
        }

        if topic_consumers.is_empty() {
            return Err(DanubeError::Unrecoverable(
                "No partitions found".to_string(),
            ));
        }

        self.consumers.extend(topic_consumers);
        Ok(())
    }

    // receive messages
    pub async fn receive(&mut self) -> Result<mpsc::Receiver<StreamMessage>> {
        // Create a channel to send messages to the client
        let (tx, rx) = mpsc::channel(100); // Buffer size of 100, adjust as needed

        // Spawn a task for each cloned TopicConsumer
        for consumer in &self.consumers {
            let tx = tx.clone();
            let consumer = consumer.clone();

            tokio::spawn(async move {
                let mut consumer = consumer.lock().await;
                while let Ok(stream) = consumer.receive().await {
                    let mut stream = stream;
                    while let Some(message) = stream.next().await {
                        match message {
                            Ok(stream_message) => {
                                if let Err(_) = tx.send(stream_message).await {
                                    // if the channel is closed exit the loop
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Error receiving message: {}", e);
                                break;
                            }
                        }
                    }
                }
            });
        }

        Ok(rx)
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
