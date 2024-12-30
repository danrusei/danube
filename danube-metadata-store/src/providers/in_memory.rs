use crate::{
    errors::Result,
    store::{MetaOptions, MetadataStore},
    watch::WatchStream,
    MetadataError,
};

use async_trait::async_trait;
use dashmap::{mapref::one::RefMut, DashMap};
use serde_json::Value;
use std::collections::BTreeMap;

/// MemoryStore is a simple in-memory key-value store that implements the MetadataStore trait.
/// SHOULD BE USED ONLY FOR TESTING PURPOSES
#[derive(Debug, Clone)]
pub struct MemoryStore {
    inner: DashMap<String, BTreeMap<String, Value>>,
}

impl MemoryStore {
    #[allow(dead_code)]
    pub async fn new() -> Result<Self> {
        Ok(MemoryStore {
            inner: DashMap::new(),
        })
    }

    fn get_map(&self, path: &str) -> Result<RefMut<String, BTreeMap<String, Value>>> {
        let parts: Vec<&str> = path.split('/').take(3).collect();
        let key = parts.join("/");

        let bmap = self.inner.entry(key.to_owned()).or_insert(BTreeMap::new());

        Ok(bmap)
    }
}

#[async_trait]
impl MetadataStore for MemoryStore {
    // Read the value of one key, identified by the path
    async fn get(&self, path: &str, _get_options: MetaOptions) -> Result<Option<Value>> {
        let bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        match bmap.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    // Return all the paths that are children to the specific path.
    async fn get_childrens(&self, path: &str) -> Result<Vec<String>> {
        let bmap = self.get_map(path)?;
        let mut child_paths = Vec::new();

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let minimum_path = parts.join("/");

        for key in bmap.keys() {
            if key.starts_with(&minimum_path)
                && key.len() > minimum_path.len()
                && key.chars().nth(minimum_path.len()).unwrap() == '/'
            {
                child_paths.push(key.clone());
            }
        }

        Ok(child_paths)
    }

    // Put a new value for a given key
    async fn put(&self, path: &str, value: Value, _put_options: MetaOptions) -> Result<()> {
        let mut bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        if key.is_empty() {
            return Err(MetadataError::Unknown("wrong path".to_string()).into());
        }

        bmap.insert(key, value);

        Ok(())
    }

    // Delete the key / value from the store
    async fn delete(&self, path: &str) -> Result<()> {
        let mut bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        if key.is_empty() {
            return Err(MetadataError::Unknown("wrong path".to_string()).into());
        }

        let _value = bmap.remove(&key);
        Ok(())
    }

    async fn watch(&self, _prefix: &str) -> Result<WatchStream> {
        unimplemented!()
    }
}

impl MemoryStore {
    // Delete a key-value pair and all the children nodes
    #[allow(dead_code)]
    async fn delete_recursive(&mut self, path: &str) -> Result<()> {
        let mut bmap = self.get_map(path)?;
        let mut keys_to_remove = Vec::new();

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let path_to_remove = parts.join("/");

        for key in bmap.keys() {
            if key.starts_with(&path_to_remove)
                && key.len() > path_to_remove.len()
                && key.chars().nth(path_to_remove.len()).unwrap() == '/'
            {
                keys_to_remove.push(key.clone());
            }
        }

        for key in keys_to_remove {
            bmap.remove(&key);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_get_delete() -> Result<()> {
        let store = MemoryStore::new().await?;

        let topic_id = "another-topic";
        let value: Value = serde_json::from_str("{\"sampling_rate\": 0.5}").unwrap();

        let path = format!("/danube/topics/{}/conf", topic_id);

        // Put a new value
        store.put(&path, value.clone(), MetaOptions::None).await?;

        // Get the value
        let retrieved_value = store.get(&path, MetaOptions::None).await?;
        assert_eq!(retrieved_value, Some(value));

        // Delete the key
        store.delete(&path).await?;

        // Try to get the value again and assert it's None (key not found)
        let result = store.get(&path, MetaOptions::None).await;
        assert!(matches!(result, Ok(None)));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() -> Result<()> {
        let store = MemoryStore::new().await?;
        let topic_id = "unknown-topic";

        // Try to get a non-existent key
        let result = store
            .get(
                format!("/danube/topics/{}/metrics", topic_id).as_str(),
                MetaOptions::None,
            )
            .await;
        // Assert that the result is None (key not found)
        match result {
            Ok(Some(_)) => {
                // If some value was found, fail the test
                panic!("Expected None (key not found), but got Some(value)");
            }
            Ok(None) => {
                // This is the expected case: key not found
                assert_eq!(result.unwrap(), None);
            }
            Err(err) => {
                // If there's an unexpected error, fail the test
                panic!("Unexpected error: {:?}", err);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_put_invalid_path() -> Result<()> {
        let store = MemoryStore::new().await?;
        let value: Value = serde_json::from_str("{\"sampling_rate\": 0.5}").unwrap();

        // Try to put with invalid path (missing segment)
        let result = store
            .put("/danube/topics", value.clone(), MetaOptions::None)
            .await;
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_recursive() -> Result<()> {
        let mut store = MemoryStore::new().await?;

        // Create a sample data structure
        store
            .put(
                "/danube/topics/topic_1/key1",
                Value::String("value1".to_string()),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_1/key2",
                Value::String("value2".to_string()),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_2/key3",
                Value::String("value3".to_string()),
                MetaOptions::None,
            )
            .await?;

        // Test deleting a path with its contents
        store.delete_recursive("/danube/topics/topic_1").await?;

        // Assert that keys under the deleted path are gone
        assert!(store
            .get("/danube/topics/topic_1/key1", MetaOptions::None)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .get("/danube/topics/topic_1/key2", MetaOptions::None)
            .await
            .unwrap()
            .is_none());

        // Assert that other directory and its key remain
        assert!(store
            .get("/danube/topics/topic_2/key3", MetaOptions::None)
            .await
            .unwrap()
            .is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_childrens() -> Result<()> {
        let store = MemoryStore::new().await?;

        // Create a sample data structure
        store
            .put(
                "/danube/topics/topic_1/key1",
                Value::String("value1".to_string()),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_2/key2",
                Value::String("value2".to_string()),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_1/subtopic/key3",
                Value::String("value3".to_string()),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/data/other/path",
                Value::String("value4".to_string()),
                MetaOptions::None,
            )
            .await?;

        // Test finding paths containing "/danube/topics/topic_1"
        let paths = store.get_childrens("/danube/topics/topic_1").await?;

        // Assert that all matching paths are found
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&"topic_1/key1".to_string()));
        assert!(paths.contains(&"topic_1/subtopic/key3".to_string()));

        // Test finding a non-existent path
        let paths = store.get_childrens("/non/existent/path").await?;

        // Assert that no paths are found
        assert!(paths.is_empty());

        Ok(())
    }
}
