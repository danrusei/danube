use anyhow::Result;

use crate::metadata_store::{MetadataStorage, MetadataStore};

#[derive(Debug)]
pub(crate) struct TopicResources {
    store: MetadataStorage,
}

impl TopicResources {
    pub(crate) fn new(store: MetadataStorage) -> Self {
        TopicResources { store }
    }
}
