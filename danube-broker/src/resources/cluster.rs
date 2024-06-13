use anyhow::Result;
use serde_json::Value;

use crate::{
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
    resources::{join_path, BASE_CLUSTER_PATH},
    LocalCache,
};

use super::BASE_UNASSIGNED_PATH;

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

    pub(crate) async fn create_cluster(&mut self, path: &str, data: String) -> Result<()> {
        let path = join_path(&[BASE_CLUSTER_PATH, path]);
        self.create(&path, serde_json::Value::String(data)).await?;
        Ok(())
    }

    pub(crate) async fn new_unassigned_topic(&mut self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_UNASSIGNED_PATH, topic_name]);
        self.create(&path, serde_json::Value::String("".to_string()))
            .await?;
        Ok(())
    }
}
