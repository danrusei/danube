use std::collections::HashMap;

#[derive(Debug, Default)]
pub(crate) struct LoadBalance {
    // mapper holds the assignmets of topics to brokers
    // Key: broker_id , Value: topic_name (/{namespace}/{topic})
    mapper: HashMap<u64, String>,
}

impl LoadBalance {
    pub(crate) fn new() -> Self {
        LoadBalance::default()
    }
    pub(crate) fn check_ownership(&self, broker_id: u64, topic_name: &str) -> bool {
        self.mapper.contains_key(&broker_id)
    }
}
