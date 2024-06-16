use crate::{
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
    LocalCache,
};
use anyhow::{Ok, Result};

mod cluster;
mod namespace;
mod topic;

pub(crate) use cluster::ClusterResources;
pub(crate) use namespace::NamespaceResources;
pub(crate) use topic::TopicResources;

pub(crate) static BASE_CLUSTER_PATH: &str = "/cluster";
pub(crate) static BASE_BROKER_PATH: &str = "/cluster/brokers";
pub(crate) static BASE_NAMESPACES_PATH: &str = "/namespaces";
pub(crate) static BASE_TOPICS_PATH: &str = "/topics";
pub(crate) static BASE_SUBSCRIPTIONS_PATH: &str = "/subscriptions";

// Once new topic is created, it is posted to unassigned path in order to be alocated by Load Manager to a broker
pub(crate) static BASE_UNASSIGNED_PATH: &str = "/cluster/unassigned";

// The Load Balance decision posted by leader Broker's Load Manager
pub(crate) static LOADBALANCE_DECISION_PATH: &str = "/cluster/load_balance";

// Cluster Leader broker_id, posted by Leader Election Service
pub(crate) static LEADER_SELECTION_PATH: &str = "/cluster/leader";

// Resources provides the mechanisms to store and retrieve specific information from MetadataStore
//
// The docs/internal_resources.md document describe how the resources are organized in Metadata Store
//
// Different starting paths create a clear hierarchical structure
// that reflects the logical organization of the Danube messaging system.
// It allows for the separation of concerns & efficient querying, ensuring that publisher, consumer, topic,
// and namespace configurations are neatly organized and do not get mixed up.
// Reduced Overhead: By limiting the scope of data retrievals to a specific path,
// the overhead of processing unnecessary data, is reduced.
#[derive(Debug, Clone)]
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
