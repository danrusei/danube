use crate::topic::Topic;
use std::error::Error;

pub(crate) struct NameSpace {}

impl NameSpace {
    // Lookup service for resolving topic names to their corresponding broker service URLs
    pub(crate) async fn get_broker_service_url(
        topic_name: Topic,
    ) -> Result<LookupResult, Box<dyn Error>> {
        todo!();
    }

    // Setting up predefined namespaces during the initialization phase of the Danube service.
    pub(crate) fn register_bootstrap_namespaces() -> Result<(), Box<dyn Error>> {
        todo!()
    }

    // Checks whether a topic exists within the specified namespace
    pub(crate) fn check_topic_exist(topic_name: Topic) -> Result<bool, Box<dyn Error>> {
        todo!()
    }

    // Checks whether the broker owns a specific topic
    pub(crate) fn check_topic_ownership(topic_name: Topic) -> Result<bool, Box<dyn Error>> {
        todo!()
    }

    // Retrieves the list of topics within the specified namespace
    pub(crate) fn get_list__of_topics(
        namespace_name: String,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        todo!()
    }

    // Retrieves the list of all partitions within the specified namespace
    pub(crate) fn get_all_partitions(
        namespace_name: String,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        todo!()
    }
}

pub(crate) enum LookupResult {
    BrokerUrl(String),
    RedirectUrl(String),
}
