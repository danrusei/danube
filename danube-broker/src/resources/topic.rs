use anyhow::{Ok, Result};
use serde_json::Value;

use crate::{
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
    policies::Policies,
    resources::BASE_TOPICS_PATH,
    schema::Schema,
    utils::join_path,
    LocalCache,
};

#[derive(Debug, Clone)]
pub(crate) struct TopicResources {
    local_cache: LocalCache,
    store: MetadataStorage,
}

impl TopicResources {
    pub(crate) fn new(local_cache: LocalCache, store: MetadataStorage) -> Self {
        TopicResources { local_cache, store }
    }
    pub(crate) async fn topic_exists(&mut self, topic_name: &str) -> Result<bool> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name]);
        let topic = self.store.get(&path, MetaOptions::None).await?;
        if topic.is_none() {
            return Ok(false);
        }

        Ok(true)
    }

    pub(crate) async fn create(&mut self, path: &str, data: Value) -> Result<()> {
        self.store.put(path, data, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) async fn add_topic_policy(
        &mut self,
        topic_name: &str,
        policies: Policies,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "policy"]);
        let data = serde_json::to_value(policies).unwrap();
        self.create(&path, data).await?;

        Ok(())
    }

    pub(crate) async fn add_topic_schema(
        &mut self,
        topic_name: &str,
        schema: Schema,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "schema"]);
        let data = serde_json::to_value(&schema).unwrap();
        self.create(&path, data).await?;

        Ok(())
    }

    pub(crate) async fn create_topic(
        &mut self,
        topic_name: &str,
        num_partitions: usize,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name]);
        //TODO! all the partitions I guess should be added
        let _ = self
            .store
            .put(&path, num_partitions.into(), MetaOptions::None)
            .await;

        Ok(())
    }

    pub(crate) fn get_schema(&self, topic_name: &str) -> Option<Schema> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "schema"]);
        let result = self.local_cache.get(&path);
        if let Some(value) = result {
            let schema: Option<Schema> = serde_json::from_value(value).ok();
            return schema;
        }
        None
    }

    pub(crate) fn get_policies(&self, topic_name: &str) -> Option<Policies> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "policy"]);
        let result = self.local_cache.get(&path);
        if let Some(value) = result {
            let policies: Option<Policies> = serde_json::from_value(value).ok();
            return policies;
        }
        None
    }
}
