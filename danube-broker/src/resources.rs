use crate::metadata_store::{MetaOptions, MetadataStorage, MetadataStore};
use anyhow::{Ok, Result};

mod cluster;
mod namespace;
mod topic;

pub(crate) use cluster::ClusterResources;
pub(crate) use namespace::NamespaceResources;
pub(crate) use topic::TopicResources;

pub(crate) static BASE_CLUSTERS_PATH: &str = "/clusters";
pub(crate) static BASE_NAMESPACE_PATH: &str = "/namespace";
pub(crate) static BASE_TOPIC_PATH: &str = "/topic";

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
    pub(crate) fn new(store: MetadataStorage) -> Self {
        Resources {
            store: store.clone(),
            cluster: ClusterResources::new(store.clone()),
            namespace: NamespaceResources::new(store.clone()),
            topic: TopicResources::new(store),
        }
    }
}

fn join_path(parts: &[&str]) -> String {
    parts.join("/")
}
