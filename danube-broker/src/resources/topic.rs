use anyhow::Result;

use crate::{
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
    resources::{join_path, BASE_TOPIC_PATH},
};

#[derive(Debug)]
pub(crate) struct TopicResources {
    store: MetadataStorage,
}

impl TopicResources {
    pub(crate) fn new(store: MetadataStorage) -> Self {
        TopicResources { store }
    }
    pub(crate) async fn topic_exists(&mut self, topic_name: &str) -> Result<bool> {
        let path = join_path(&[BASE_TOPIC_PATH, topic_name]);
        let topic = self.store.get(&path, MetaOptions::None).await?;
        if topic.is_null() {
            return Ok(false);
        }

        Ok(true)
    }
    pub(crate) async fn create_topic(
        &mut self,
        topic_name: &str,
        num_partitions: usize,
    ) -> Result<String> {
        todo!()
    }
    async fn is_partitioned(topic_name: &str) -> Result<bool> {
        todo!()
    }
}
