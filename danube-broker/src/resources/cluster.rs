use anyhow::Result;

use crate::{
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
    resources::{join_path, BASE_CLUSTERS_PATH},
};

#[derive(Debug)]
pub(crate) struct ClusterResources {
    store: MetadataStorage,
}

impl ClusterResources {
    pub(crate) fn new(store: MetadataStorage) -> Self {
        ClusterResources { store }
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
