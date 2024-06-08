use serde::{Deserialize, Serialize};

// LoadReport holds information that are required by Load Manager
// to take the topics allocation decision to brokers
//
// The broker periodically reports its load metrics to Metadata Store.
// In this struct can be added any information that can serve to Load Manager
// to take a better decision in the allocation of topics/partitions to brokers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LoadReport {
    // here can be added all the information
    pub(crate) resources_usage: Vec<SystemLoad>,
    // number of topics served by the broker
    pub(crate) topics: usize,
    // the list of topic_name (/{namespace}/{topic}) served by the broker
    pub(crate) topic_list: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SystemLoad {
    pub(crate) resource: ResourceType,
    pub(crate) usage: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ResourceType {
    CPU,
    Memory,
}
