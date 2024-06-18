use crate::{consumer::Consumer, producer::Producer};

use anyhow::{anyhow, Result};
use danube_client::{
    Consumer as ClientConsumer, DanubeClient, Producer as ClientProducer, SchemaType, SubType,
};
use serde::{Deserialize, Serialize};
use tracing::info;

pub(crate) static META_SYNC: &str = "/system/meta-sync";
pub(crate) static SUBSCRIPTION_NAME: &str = "metadata-sync";

// The synchronizer ensures that metadata and configuration settings across different brokers remain consistent.
// It propagates changes to metadata and configuration settings using client Producers and Consumers.
// This is in addition to Metadata Storage watch events, allowing brokers to process metadata updates
// even if there was a communication glitch or the broker was unavailable for a short period, potentially missing the Store Watch events.
// The synchronizer allows for dynamic updates to configuration settings without requiring a broker service restart.
#[derive(Debug)]
pub(crate) struct Syncronizer {
    client: Option<DanubeClient>,
    consumer: Option<ClientConsumer>,
    producer: Option<ClientProducer>,
}

impl Syncronizer {
    pub(crate) fn new() -> Self {
        Syncronizer {
            client: None,
            consumer: None,
            producer: None,
        }
    }
    pub(crate) fn with_client(&mut self, client: DanubeClient) -> Result<()> {
        self.client = Some(client);
        Ok(())
    }
    async fn create_producer(&mut self) -> Result<()> {
        let client = if let Some(client) = &self.client {
            client
        } else {
            return Err(anyhow!("Unable to get the DanubeClient"));
        };

        let mut producer = client
            .new_producer()
            .with_topic(META_SYNC)
            .with_name("test_producer1")
            .with_schema("my_app".into(), SchemaType::Bytes)
            .build();

        let prod_id = producer.create().await?;
        self.producer = Some(producer);
        info!(
            "The Syncronozer Producer has been created with ID: {:?}",
            prod_id
        );
        Ok(())
    }
    async fn create_consumer(&mut self) -> Result<()> {
        let client = if let Some(client) = &self.client {
            client
        } else {
            return Err(anyhow!("Unable to get the DanubeClient"));
        };

        let mut consumer = client
            .new_consumer()
            .with_topic(META_SYNC)
            .with_consumer_name("")
            .with_subscription(SUBSCRIPTION_NAME)
            .with_subscription_type(SubType::Shared)
            .build();

        // Subscribe to the topic
        let consumer_id = consumer.subscribe().await?;
        self.consumer = Some(consumer);
        println!(
            "The  Syncronizer Consumer with ID: {:?} was created",
            consumer_id
        );

        Ok(())
    }

    async fn notify(&self, event: MetadataEvent) -> Result<()> {
        // Serialize the event into a Vec<u8>
        let serialized_data = serde_json::to_vec(&event)?;

        if let Some(producer) = &self.producer {
            let message_id = producer.send(serialized_data).await?;
            info!(
                "Successfully sent the notification of the metadata change event {:?}",
                event
            );
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MetadataEvent {
    path: String,
    value: Vec<u8>,
    notification_type: NotificationType,
    // add other fields part of the ETCD get/put/watch...like lastUpdatedTimestamp and version
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum NotificationType {
    Created,
    Modified,
    Deleted,
}
