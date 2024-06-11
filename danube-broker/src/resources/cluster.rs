use anyhow::Result;

use crate::{
    controller::LocalCache,
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
    resources::{join_path, BASE_CLUSTERS_PATH},
};

#[derive(Debug, Clone)]
pub(crate) struct ClusterResources {
    local_cache: LocalCache,
    store: MetadataStorage,
}

impl ClusterResources {
    pub(crate) fn new(local_cache: LocalCache, store: MetadataStorage) -> Self {
        ClusterResources { local_cache, store }
    }

    pub(crate) async fn create_cluster(&mut self, path: &str, data: String) -> Result<()> {
        self.create(&join_path(&[BASE_CLUSTERS_PATH, path]), data)
            .await?;
        Ok(())
    }

    pub(crate) async fn create(&mut self, path: &str, data: String) -> Result<()> {
        self.store
            .put(path, serde_json::Value::String(data), MetaOptions::None)
            .await?;
        Ok(())
    }
}
