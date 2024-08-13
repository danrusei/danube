use crate::{
    errors::Result, message_router::MessageRouter, topic_producer::TopicProducer, DanubeClient,
    Schema, SchemaType,
};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Represents a Producer
#[derive(Debug)]
pub struct Producer {
    client: DanubeClient,
    topic_name: String,
    schema: Schema,
    producer_name: String,
    partitions: Option<usize>,
    message_router: Option<MessageRouter>,
    producers: Arc<Mutex<Vec<TopicProducer>>>,
    producer_options: ProducerOptions,
}

impl Producer {
    pub(crate) fn new(
        client: DanubeClient,
        topic_name: String,
        schema: Option<Schema>,
        producer_name: String,
        partitions: Option<usize>,
        message_router: Option<MessageRouter>,
        producer_options: ProducerOptions,
    ) -> Self {
        // default schema is String if not specified
        let schema = if let Some(sch) = schema {
            sch
        } else {
            Schema::new("string_schema".into(), SchemaType::String)
        };

        Producer {
            client,
            topic_name,
            schema,
            producer_name,
            partitions,
            message_router,
            producers: Arc::new(Mutex::new(Vec::new())),
            producer_options,
        }
    }

    pub async fn create(&mut self) -> Result<()> {
        let mut topic_producers: Vec<_> = match self.partitions {
            None => {
                // Create a single TopicProducer for non-partitioned topic
                vec![TopicProducer::new(
                    self.client.clone(),
                    self.topic_name.clone(),
                    self.producer_name.clone(),
                    self.schema.clone(),
                    self.producer_options.clone(),
                )]
            }
            Some(partitions) => {
                if self.message_router.is_none() {
                    self.message_router = Some(MessageRouter::new(partitions));
                };

                (0..partitions)
                    .map(|partition_id| {
                        let topic = format!("{}-part-{}", self.topic_name, partition_id);
                        TopicProducer::new(
                            self.client.clone(),
                            topic,
                            format!("{}-{}", self.producer_name, partition_id),
                            self.schema.clone(),
                            self.producer_options.clone(),
                        )
                    })
                    .collect()
            }
        };

        for topic_producer in &mut topic_producers {
            let _prod_id = topic_producer.create().await?;
        }

        // ensure that the producers are added only if all topic_producers are succesfully created
        let mut producers = self.producers.lock().await;
        *producers = topic_producers;

        Ok(())
    }

    pub async fn send(
        &self,
        data: Vec<u8>,
        attributes: Option<HashMap<String, String>>,
    ) -> Result<u64> {
        let next_partition = &self
            .message_router
            .as_ref()
            .expect("already initialized")
            .round_robin();
        let producers = self.producers.lock().await;

        let sequence_id = producers[next_partition.to_owned()]
            .send(data, attributes)
            .await?;

        Ok(sequence_id)
    }
}

#[derive(Debug, Clone)]
pub struct ProducerBuilder {
    client: DanubeClient,
    topic: Option<String>,
    num_partitions: Option<usize>,
    producer_name: Option<String>,
    schema: Option<Schema>,
    producer_options: ProducerOptions,
}

impl ProducerBuilder {
    pub fn new(client: &DanubeClient) -> Self {
        ProducerBuilder {
            client: client.clone(),
            topic: None,
            num_partitions: None,
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

    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.num_partitions = Some(partitions);
        self
    }

    pub fn build(self) -> Producer {
        let topic_name = self
            .topic
            .expect("can't create a producer without assigning to a topic");
        let producer_name = self
            .producer_name
            .expect("you should provide a name to the created producer");

        Producer::new(
            self.client,
            topic_name,
            self.schema,
            producer_name,
            self.num_partitions,
            None,
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
