use crate::metadata_store::{MetaOptions, MetadataStorage, MetadataStore};
use anyhow::{Ok, Result};

// Different starting paths create a clear hierarchical structure
// that reflects the logical organization of the Danube messaging system.
// It allows for the separation of concerns & efficient querying, ensuring that publisher, consumer, topic,
// and namespace configurations are neatly organized and do not get mixed up.
// Reduced Overhead: By limiting the scope of data retrievals to a specific path,
// the overhead of processing unnecessary data, is reduced.
//
// Namespace:
// /namespace/{namespace}/{policy-name}
// Ex Key: /namespace/markets/retention that stores a Json like { "retentionTimeInMinutes": 1440 }
// Ex Key: /namespace/markets/config that stores the namespace configuration
//
// Topic:
// /topic/{namespace}/{topic}/{policy-name}
// Ex Key: /topic/markets/trade-events/maxConsumers that host an Json like { "maxConsumers": 20 }
// Ex Key: /topic/markets/trade-events/subscriptions that stores the names of all subscriptions
// Ex Key: /topic/markets/trade-events/consig that stores the topic config
//
// Same for Subscription, Publisher & Consumers
// /publisher/{namespace}/{topic}/{publisher-id}/config -> /publisher/markets/trade-events/publisher-123/config
// /consumer/{namespace}/{topic}/{consumer-id}/config -> /consumer/markets/trade-events/consumer-456/config
//
// Resources provides the mechanisms to store and retrieve specific information from MetadataStore

static BASE_CLUSTERS_PATH: &str = "/clusters";

#[derive(Debug)]
pub(crate) struct Resources {
    store: MetadataStorage,
    pub(crate) cluster: ClusterResources,
    pub(crate) namespace: NamespaceResources,
    pub(crate) topic: TopicResources,
    // should hold also the MetadataStore,
    // as the resources translate the Danube requests into MetadataStore paths puts & gets
}

impl Resources {
    pub(crate) fn new(store: MetadataStorage) -> Self {
        Resources {
            store: store.clone(),
            cluster: ClusterResources::new(store.clone()),
            namespace: NamespaceResources::new(store.clone()),
            topic: TopicResources::new(store),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ClusterResources {
    store: MetadataStorage,
}

impl ClusterResources {
    pub(crate) fn new(store: MetadataStorage) -> Self {
        ClusterResources { store }
    }

    pub(crate) async fn create_cluster(&mut self, path: &str, data: String) -> Result<()> {
        self.create(&join_path(&[BASE_CLUSTERS_PATH, path]), data)
            .await?;
        Ok(())
    }

    pub(crate) async fn create(&mut self, path: &str, data: String) -> Result<()> {
        self.store
            .put(path, serde_json::Value::String(data), MetaOptions::None)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct NamespaceResources {
    store: MetadataStorage,
}

impl NamespaceResources {
    pub(crate) fn new(store: MetadataStorage) -> Self {
        NamespaceResources { store }
    }
}

#[derive(Debug)]
pub(crate) struct TopicResources {
    store: MetadataStorage,
}

impl TopicResources {
    pub(crate) fn new(store: MetadataStorage) -> Self {
        TopicResources { store }
    }
}

fn join_path(parts: &[&str]) -> String {
    parts.join("/")
}
