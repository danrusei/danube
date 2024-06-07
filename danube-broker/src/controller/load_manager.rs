use anyhow::Result;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub(crate) struct LoadManager {
    // mapper holds the assignmets of topics to brokers
    // Key: broker_id , Value: topic_name (/{namespace}/{topic})
    mapper: HashMap<u64, String>,
}

impl LoadManager {
    pub(crate) fn new() -> Self {
        LoadManager::default()
    }
    pub(crate) fn check_ownership(&self, broker_id: u64, topic_name: &str) -> bool {
        self.mapper.contains_key(&broker_id)
    }
    pub(crate) fn start(&self) -> Result<()> {
        todo!()
    }
}
