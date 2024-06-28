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
    pub(crate) topics_len: usize,
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

pub(crate) fn generate_load_report(topics_len: usize, topic_list: Vec<String>) -> LoadReport {
    // mock system resource susage for now
    let system_load: Vec<SystemLoad> = get_system_resource_usage();

    LoadReport {
        resources_usage: system_load,
        topics_len,
        topic_list,
    }
}

pub(crate) fn get_system_resource_usage() -> Vec<SystemLoad> {
    let mut system_load = Vec::new();
    let cpu_usage = SystemLoad {
        resource: ResourceType::CPU,
        usage: 30,
    };
    let mem_usage = SystemLoad {
        resource: ResourceType::Memory,
        usage: 30,
    };
    system_load.push(cpu_usage);
    system_load.push(mem_usage);

    system_load
}
