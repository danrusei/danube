use anyhow::Result;
use serde_json::Value;

use crate::{
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
    resources::{BASE_BROKER_PATH, BASE_CLUSTER_PATH},
    utils::join_path,
    LocalCache,
};

use super::{BASE_REGISTER_PATH, BASE_UNASSIGNED_PATH};

#[derive(Debug, Clone)]
pub(crate) struct ClusterResources {
    local_cache: LocalCache,
    store: MetadataStorage,
}

impl ClusterResources {
    pub(crate) fn new(local_cache: LocalCache, store: MetadataStorage) -> Self {
        ClusterResources { local_cache, store }
    }

    pub(crate) async fn create(&mut self, path: &str, data: Value) -> Result<()> {
        self.store.put(path, data, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) async fn create_cluster(&mut self, path: &str) -> Result<()> {
        let path = join_path(&[BASE_CLUSTER_PATH, path]);
        self.create(&path, serde_json::Value::Null).await?;
        Ok(())
    }

    pub(crate) async fn new_unassigned_topic(&mut self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_UNASSIGNED_PATH, topic_name]);
        self.create(&path, serde_json::Value::Null).await?;
        Ok(())
    }

    // search all the paths for the topic name and return broker_id
    // search for /{namespace}/{topic} as part of the  /cluster/brokers/*
    // example /cluster/brokers/{broker_id}/{namespace}/{topic})
    pub(crate) async fn get_broker_for_topic(&self, topic_name: &str) -> Option<String> {
        let keys = self
            .local_cache
            .get_keys_with_prefix(&BASE_BROKER_PATH)
            .await;
        for path in keys {
            if let Some(pos) = path.find(topic_name) {
                let parts: Vec<&str> = path[..pos].split('/').collect();
                if parts.len() > 3 {
                    let broker_id = parts[3];
                    return Some(broker_id.to_string());
                }
            }
        }
        None
    }

    pub(crate) fn get_brokers(&self) -> Vec<String> {
        todo!()
    }

    pub(crate) fn get_broker_addr(&self, broker_id: &str) -> Option<String> {
        let path = join_path(&[BASE_REGISTER_PATH, broker_id]);
        let value = self.local_cache.get(&path);
        if let Some(value) = value {
            match value {
                Value::String(broker_addr) => return Some(broker_addr),

                _ => return None,
            }
        }
        None
    }

    pub(crate) fn get_broker_info(&self, broker_id: &str) -> Option<(String, String, String)> {
        todo!()
    }
}
