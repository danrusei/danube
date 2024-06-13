use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde_json::{from_value, Value};

use crate::{
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
    policies::Policies,
    resources::{join_path, BASE_NAMESPACES_PATH},
    LocalCache,
};

#[derive(Debug, Clone)]
pub(crate) struct NamespaceResources {
    local_cache: LocalCache,
    store: MetadataStorage,
}

impl NamespaceResources {
    pub(crate) fn new(local_cache: LocalCache, store: MetadataStorage) -> Self {
        NamespaceResources { local_cache, store }
    }

    pub(crate) async fn namespace_exist(&mut self, namespace_name: &str) -> Result<bool> {
        let path = join_path(&[BASE_NAMESPACES_PATH, namespace_name]);
        let value = self.store.get(&path, MetaOptions::None).await?;
        if value.is_null() {
            return Ok(false);
        }
        Ok(true)
    }

    pub(crate) async fn create_policies(
        &mut self,
        namespace_name: &str,
        policies: Policies,
    ) -> Result<()> {
        let policies_map = policies.get_fields_as_map();
        for (key, value) in policies_map {
            let path = join_path(&[BASE_NAMESPACES_PATH, namespace_name, &key]);
            self.create(&path, value).await?;
        }
        Ok(())
    }

    pub(crate) fn get_policies(&mut self, namespace_name: &str) -> Result<Policies> {
        let path = join_path(&[BASE_NAMESPACES_PATH, namespace_name, "policy"]);
        let result = self.local_cache.get(&path);
        let value = if let Some(value) = result {
            value
        } else {
            return Err(anyhow!("Unable to retrive the policies for the namespace"));
        };

        let policies: Policies = serde_json::from_value(value)?;

        Ok(policies)
    }

    pub(crate) async fn create(&mut self, path: &str, data: Value) -> Result<()> {
        self.store.put(path, data, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) fn check_if_topic_exist(&self, ns_name: &str, topic_name: &str) -> bool {
        let path = join_path(&[BASE_NAMESPACES_PATH, ns_name, "topics"]);

        match self.local_cache.get(&path) {
            Some(value) => {
                // Attempt to deserialize the Value into a Vec<String>.
                let topics_name: Vec<String> =
                    from_value(value.clone()).expect(&format!("Unable to deserialize {}", path));
                if topics_name.contains(&topic_name.to_owned()) {
                    return true;
                }
            }
            None => return false,
        }

        false
    }

    pub(crate) async fn create_new_topic(&mut self, topic_name: &str) -> Result<()> {
        let parts: Vec<_> = topic_name.split("/").collect();
        let ns_name = parts[1];
        let path = join_path(&[BASE_NAMESPACES_PATH, ns_name, "topics", topic_name]);

        self.create(&path, serde_json::Value::String("".to_string()))
            .await?;

        Ok(())
    }
}
