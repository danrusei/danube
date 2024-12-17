use crate::{
    errors::{DanubeError, Result},
    topic_consumer::TopicConsumer,
    DanubeClient, MessageID, StreamMessage,
};

use futures::{future::join_all, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Represents the type of subscription
///
/// Variants:
/// - `Exclusive`: Only one consumer can subscribe to the topic at a time.
/// - `Shared`: Multiple consumers can subscribe to the topic concurrently.
/// - `FailOver`: Only one consumer can subscribe to the topic at a time,
///             multiple can subscribe but waits in standby, if the active consumer disconnect        
#[derive(Debug, Clone)]
pub enum SubType {
    Exclusive,
    Shared,
    FailOver,
}

/// Consumer represents a message consumer that subscribes to a topic and receives messages.
/// It handles communication with the message broker and manages the consumer's state.
#[derive(Debug)]
pub struct Consumer {
    // the Danube client
    client: DanubeClient,
    // the topic name, from where the messages are consumed
    topic_name: String,
    // the name of the Consumer
    consumer_name: String,
    // the map between the partitioned topic name and the consumer instance
    consumers: HashMap<String, Arc<Mutex<TopicConsumer>>>,
    // the name of the subscription the consumer is attached to
    subscription: String,
    // the type of the subscription, that can be Shared and Exclusive
    subscription_type: SubType,
    // other configurable options for the consumer
    consumer_options: ConsumerOptions,
}

impl Consumer {
    pub(crate) fn new(
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
            consumers: HashMap::new(),
            subscription,
            subscription_type,
            consumer_options,
        }
    }

    /// Initializes the subscription to a non-partitioned or partitioned topic and starts the health check service.
    ///
    /// This function establishes a gRPC connection with the brokers and requests to subscribe to the specified topic.
    ///
    /// # Errors
    /// If an error occurs during subscription or initialization, it is returned as part of the `Err` variant.
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
                    Ok(_) => Ok(topic_consumer),
                    Err(e) => Err(e),
                }
            });

            tasks.push(task);
        }

        // Wait for all tasks to complete
        let results = join_all(tasks).await;

        // Collect results
        let mut topic_consumers = HashMap::new();
        for result in results {
            match result {
                Ok(Ok(consumer)) => {
                    topic_consumers.insert(
                        consumer.get_topic_name().to_string(),
                        Arc::new(Mutex::new(consumer)),
                    );
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(DanubeError::Unrecoverable(e.to_string())),
            }
        }

        if topic_consumers.is_empty() {
            return Err(DanubeError::Unrecoverable(
                "No partitions found".to_string(),
            ));
        }

        self.consumers.extend(topic_consumers.into_iter());
        Ok(())
    }

    /// Starts receiving messages from the subscribed partitioned or non-partitioned topic.
    ///
    /// This function continuously polls for new messages and handles them as long as the `stop_signal` has not been set to `true`.
    ///
    /// # Returns
    ///
    /// A `Result` with:
    /// - `Ok(mpsc::Receiver<StreamMessage>)` if the receive client is successfully created and ready to receive messages.
    /// - `Err(e)` if the receive client cannot be created or if other issues occur.
    pub async fn receive(&mut self) -> Result<mpsc::Receiver<StreamMessage>> {
        // Create a channel to send messages to the client
        let (tx, rx) = mpsc::channel(100); // Buffer size of 100, adjust as needed

        // Spawn a task for each cloned TopicConsumer
        for (_, consumer) in &self.consumers {
            let tx = tx.clone();
            let consumer = consumer.clone();

            tokio::spawn(async move {
                let mut consumer = consumer.lock().await;
                while let Ok(stream) = consumer.receive().await {
                    let mut stream = stream;
                    while let Some(message) = stream.next().await {
                        match message {
                            Ok(stream_message) => {
                                let message: StreamMessage = stream_message.into();
                                if let Err(_) = tx.send(message).await {
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

    pub async fn ack(&mut self, req_id: u64, msg_id: MessageID) -> Result<()> {
        let topic_consumer = self.consumers.get_mut(&self.topic_name);
        if let Some(topic_consumer) = topic_consumer {
            let mut topic_consumer = topic_consumer.lock().await;
            let _ = topic_consumer.send_ack(req_id, msg_id).await?;
        }
        Ok(())
    }
}

/// ConsumerBuilder is a builder for creating a new Consumer instance.
///
/// It allows setting various properties for the consumer such as topic, name, subscription,
/// subscription type, and options.
#[derive(Debug, Clone)]
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

    /// Sets the topic name for the consumer.
    ///
    /// This method specifies the topic that the consumer will subscribe to. It is a required field and must be set before the consumer can be created.
    ///
    /// # Parameters
    ///
    /// - `topic`: The name of the topic for the consumer. This should be a non-empty string that corresponds to an existing topic.
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Sets the name of the consumer instance.
    ///
    /// This method specifies the name to be assigned to the consumer. It is a required field and must be set before the consumer can be created.
    ///
    /// # Parameters
    ///
    /// - `consumer_name`: The name for the consumer instance. This should be a non-empty string that uniquely identifies the consumer.
    pub fn with_consumer_name(mut self, consumer_name: impl Into<String>) -> Self {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    /// Sets the name of the subscription for the consumer.
    ///
    /// This method specifies the subscription that the consumer will use. It is a required field and must be set before the consumer can be created.
    ///
    /// # Parameters
    ///
    /// - `subscription_name`: The name of the subscription. This should be a non-empty string that identifies the subscription to which the consumer will be subscribed.
    pub fn with_subscription(mut self, subscription_name: impl Into<String>) -> Self {
        self.subscription = Some(subscription_name.into());
        self
    }

    /// Sets the type of subscription for the consumer. This field is optional.
    ///
    /// This method specifies the type of subscription that the consumer will use. The subscription type determines how messages are distributed to consumers that share the same subscription.
    ///
    /// # Parameters
    ///
    /// - `sub_type`: The type of subscription. This should be one of the following:
    ///   - `SubType::Exclusive`: The consumer exclusively receives all messages for the subscription.
    ///   - `SubType::Shared`: Messages are distributed among multiple consumers sharing the same subscription. Default if not specified.
    ///   - `SubType::FailOver`: Only one consumer receives messages, and if it fails, another consumer takes over.
    pub fn with_subscription_type(mut self, subscription_type: SubType) -> Self {
        self.subscription_type = Some(subscription_type);
        self
    }

    /// Creates a new `Consumer` instance using the settings configured in the `ConsumerBuilder`.
    ///
    /// This method performs validation to ensure that all required fields are set before creating the `Consumer`.  Once validation is successful, it constructs and returns a new `Consumer` instance configured with the specified settings.
    ///
    /// # Returns
    ///
    /// -  A `Consumer` instance if the builder configuration is valid and the consumer is created successfully.
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
