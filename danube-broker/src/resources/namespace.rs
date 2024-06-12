use std::collections::HashMap;

use anyhow::Result;
use serde_json::{from_value, Value};

use crate::{
    metadata_store::{MetaOptions, MetadataStorage, MetadataStore},
    policies::Policies,
    resources::{join_path, BASE_NAMESPACE_PATH},
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
        let path = join_path(&[BASE_NAMESPACE_PATH, namespace_name]);
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
            let path = join_path(&[BASE_NAMESPACE_PATH, namespace_name, &key]);
            self.create(&path, value).await?;
        }
        Ok(())
    }

    pub(crate) async fn get_policies(&mut self, namespace_name: &str) -> Result<Policies> {
        let path = join_path(&[BASE_NAMESPACE_PATH, namespace_name]);
        let pols = self.store.get_childrens(&path).await?;
        let mut map: HashMap<String, Value> = HashMap::new();
        for pol_name in pols {
            let path = join_path(&[BASE_NAMESPACE_PATH, namespace_name, &pol_name]);
            let v = self.store.get(&path, MetaOptions::None).await?;
            map.insert(pol_name, v);
        }
        Ok(Policies::from_hashmap(map)?)
    }

    pub(crate) async fn create(&mut self, path: &str, data: Value) -> Result<()> {
        self.store.put(path, data, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) fn check_if_topic_exist(&self, ns_name: &str, topic_name: &str) -> bool {
        let path = join_path(&[BASE_NAMESPACE_PATH, ns_name, "topics"]);

        match self.local_cache.namespaces.get(&path) {
            Some(value) => {
                // Attempt to deserialize the Value into a Vec<String>.
                let topics_name: Vec<String> =
                    from_value(value.1.clone()).expect(&format!("Unable to deserialize {}", path));
                if topics_name.contains(&topic_name.to_owned()) {
                    return true;
                }
            }
            None => return false,
        }

        false
    }
}
