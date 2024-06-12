use crate::{consumer::Consumer, producer::Producer};

use anyhow::Result;
use danube_client::{
    Consumer as ClientConsumer, DanubeClient, Producer as ClientProducer, SchemaType, SubType,
};
use serde::{Deserialize, Serialize};
use tracing::info;

pub(crate) static META_SYNC: &str = "/system/meta-sync";
pub(crate) static SUBSCRIPTION_NAME: &str = "metadata-sync";

// The synchronizer ensures that metadata & configuration settings across different brokers remains consistent.
// It helps in propagating changes to metadata & configuration settings, using the client Producers and Consumers.
// This is in addition to Metadata Storage watch events, allowing brokers to process metadata updates,
// even if there was a communication glitch or the broker was unavailable for a short period, so potentially missed the Store Watch events.
// The synchronizer allows for dynamic updates to configuration settings without requiring a restart of the broker service.
#[derive(Debug)]
pub(crate) struct Syncronizer {
    client: DanubeClient,
    consumer: Option<ClientConsumer>,
    producer: Option<ClientProducer>,
}

impl Syncronizer {
    pub(crate) fn new() -> Result<Self> {
        let client = DanubeClient::builder()
            .service_url("http://[::1]:6650")
            .build()?;

        Ok(Syncronizer {
            client,
            consumer: None,
            producer: None,
        })
    }
    async fn create_producer(&mut self) -> Result<()> {
        let mut producer = self
            .client
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
        let mut consumer = self
            .client
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
