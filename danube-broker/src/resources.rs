use crate::{
    controller::LocalCache,
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
};
use anyhow::{Ok, Result};

mod cluster;
mod namespace;
mod topic;

pub(crate) use cluster::ClusterResources;
pub(crate) use namespace::NamespaceResources;
pub(crate) use topic::TopicResources;

pub(crate) static BASE_CLUSTERS_PATH: &str = "/cluster";
pub(crate) static BASE_BROKER_PATH: &str = "/cluster/broker";
pub(crate) static BASE_NAMESPACE_PATH: &str = "/namespace";
pub(crate) static BASE_TOPIC_PATH: &str = "/topic";
pub(crate) static BASE_SUBSCRIPTION_PATH: &str = "/subscription";
pub(crate) static BASE_PRODUCER_PATH: &str = "/producer";

pub(crate) static LOADBALACE_DECISION_PATH: &str = "/cluster/load_balance";

// Resources provides the mechanisms to store and retrieve specific information from MetadataStore
//
// The resources/_resources.md document describe how the resources are organized in Metadata Store
//
// Different starting paths create a clear hierarchical structure
// that reflects the logical organization of the Danube messaging system.
// It allows for the separation of concerns & efficient querying, ensuring that publisher, consumer, topic,
// and namespace configurations are neatly organized and do not get mixed up.
// Reduced Overhead: By limiting the scope of data retrievals to a specific path,
// the overhead of processing unnecessary data, is reduced.
#[derive(Debug)]
pub(crate) struct Resources {
    store: MetadataStorage,
    pub(crate) cluster: ClusterResources,
    pub(crate) namespace: NamespaceResources,
    pub(crate) topic: TopicResources,
    // should hold also the MetadataStore,
    // as the resources translate the Danube requests into MetadataStore paths puts & gets
}

// A wrapper for interacting with Metadata Storage
impl Resources {
    pub(crate) fn new(local_cache: LocalCache, store: MetadataStorage) -> Self {
        Resources {
            store: store.clone(),
            cluster: ClusterResources::new(local_cache.clone(), store.clone()),
            namespace: NamespaceResources::new(local_cache.clone(), store.clone()),
            topic: TopicResources::new(local_cache, store),
        }
    }
}

pub(crate) fn join_path(parts: &[&str]) -> String {
    parts.join("/")
}
