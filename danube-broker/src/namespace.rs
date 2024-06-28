use anyhow::Result;

pub(crate) static DEFAULT_NAMESPACE: &str = "default";
pub(crate) static SYSTEM_NAMESPACE: &str = "system";

// NameSpace - coordinates topics ownership
#[derive(Debug, Default)]
#[allow(dead_code)]
pub(crate) struct NameSpace {
    name: String,
    // list of topic_name associated with the Namespace
    topics: Vec<String>,
}

impl NameSpace {
    // Setting up predefined namespaces during the initialization phase of the Danube service.
    #[allow(dead_code)]
    pub(crate) fn register_bootstrap_namespaces() -> Result<()> {
        todo!()
    }

    // Checks whether a topic exists within the specified namespace
    #[allow(dead_code)]
    pub(crate) fn check_topic_exist(&self, topic_name: &str) -> bool {
        self.topics.contains(&topic_name.to_owned())
    }

    // Retrieves the list of topics within the specified namespace
    #[allow(dead_code)]
    pub(crate) fn get_list_of_topics(&self, _namespace_name: &str) -> Vec<String> {
        self.topics.clone()
    }

    // Retrieves the list of all partitions within the specified namespace
    #[allow(dead_code)]
    pub(crate) fn get_all_partitions(&self, _namespace_name: &str) -> Vec<String> {
        self.topics.clone()
    }
}
