use anyhow::Result;

use crate::{
    controller::LocalCache,
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
    resources::{join_path, BASE_TOPIC_PATH},
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
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPIC_PATH, topic_name]);
        //TODO! all the partitions I guess should be added
        self.store
            .put(&path, num_partitions.into(), MetaOptions::None);

        Ok(())
    }
}
