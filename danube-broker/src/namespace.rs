use crate::topic::Topic;
use anyhow::Result;
use std::error::Error;

pub(crate) static DEFAULT_NAMESPACE: &str = "default";
pub(crate) static SYSTEM_NAMESPACE: &str = "system";

// NameSpace - coordinates topics ownership
#[derive(Debug, Default)]
pub(crate) struct NameSpace {
    name: String,
    //topics name associated with the Namespace
    topics: Vec<String>,
}

impl NameSpace {
    // Lookup service for resolving topic names to their corresponding broker service URLs
    pub(crate) async fn get_broker_service_url(topic_name: &str) -> Result<LookupResult> {
        todo!();
    }

    // Setting up predefined namespaces during the initialization phase of the Danube service.
    pub(crate) fn register_bootstrap_namespaces() -> Result<()> {
        todo!()
    }

    // Checks whether a topic exists within the specified namespace
    pub(crate) fn check_topic_exist(&self, topic_name: &str) -> bool {
        self.topics.contains(&topic_name.to_owned())
    }

    // Retrieves the list of topics within the specified namespace
    pub(crate) fn get_list_of_topics(namespace_name: &str) -> Result<Vec<String>> {
        todo!()
    }

    // Retrieves the list of all partitions within the specified namespace
    pub(crate) fn get_all_partitions(namespace_name: &str) -> Result<Vec<String>> {
        todo!()
    }
}

pub(crate) enum LookupResult {
    BrokerUrl(String),
    RedirectUrl(String),
}
