use dashmap::mapref::one::RefMut;
use dashmap::DashMap;
use serde_json::Value;
use std::collections::BTreeMap;
use std::error::Error;

use crate::metadata_store::MetadataStore;

#[derive(Debug)]
pub(crate) struct LocalMemoryMetadataStore {
    inner: DashMap<String, BTreeMap<String, Value>>,
}

impl LocalMemoryMetadataStore {
    fn new() -> Self {
        LocalMemoryMetadataStore {
            inner: DashMap::new(),
        }
    }

    fn get_map(
        &self,
        path: &str,
    ) -> Result<RefMut<String, BTreeMap<String, Value>>, Box<dyn Error>> {
        let parts: Vec<&str> = path.split('/').take(3).collect();
        let key = parts.join("/");

        let bmap = self.inner.entry(key.to_owned()).or_insert(BTreeMap::new());

        Ok(bmap)
    }
}

impl MetadataStore for LocalMemoryMetadataStore {
    // Read the value of one key, identified by the path
    async fn get(&mut self, path: &str) -> Result<Value, Box<dyn Error>> {
        let bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        match bmap.get(&key) {
            Some(value) => Ok(value.clone()),
            None => Err(From::from("Key not found")),
        }
    }

    // Return all the paths that are children to the specific path.
    async fn get_childrens(&mut self, path: &str) -> Result<Vec<String>, Box<dyn Error>> {
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
    async fn put(&mut self, path: &str, value: Value) -> Result<(), Box<dyn Error>> {
        let mut bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        if key.is_empty() {
            return Err(From::from("wrong path"));
        }

        bmap.insert(key, value);

        Ok(())
    }

    // Delete the key / value from the store
    async fn delete(&mut self, path: &str) -> Result<Option<Value>, Box<dyn Error>> {
        let mut bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        if key.is_empty() {
            return Err(From::from("wrong path"));
        }

        let value = bmap.remove(&key);
        Ok(value)
    }

    // Delete a key-value pair and all the children nodes
    async fn delete_recursive(&mut self, path: &str) -> Result<(), Box<dyn Error>> {
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
            let _ = bmap.remove(&key).ok_or("unable to remove key".to_owned())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_get_delete() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = LocalMemoryMetadataStore::new();

        let topic_id = "another-topic";
        let value: Value = serde_json::from_str("{\"sampling_rate\": 0.5}").unwrap();

        let path = format!("/danube/topics/{}/conf", topic_id);

        // Put a new value
        store.put(&path, value.clone()).await?;

        // Get the value
        let retrieved_value = store.get(&path).await?;
        assert_eq!(retrieved_value, value);

        // Delete the key
        store.delete(&path).await?;

        // Try to get the value again and assert it's an error (key not found)
        assert!(store.get(&path).await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        let mut store = LocalMemoryMetadataStore::new();
        let topic_id = "unknown-topic";

        // Try to get a non-existent key
        let result = store
            .get(format!("/danube/topics/{}/metrics", topic_id).as_str())
            .await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().to_string(), "Key not found");
    }

    #[tokio::test]
    async fn test_put_invalid_path() {
        let mut store = LocalMemoryMetadataStore::new();
        let value: Value = serde_json::from_str("{\"sampling_rate\": 0.5}").unwrap();

        // Try to put with invalid path (missing segment)
        let result = store.put("/danube/topics", value.clone()).await;
        assert!(result.is_err());
    }
    #[tokio::test]
    async fn test_delete_recursive() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = LocalMemoryMetadataStore::new();

        // Create a sample data structure
        store
            .put(
                "/danube/topics/topic_1/key1",
                Value::String("value1".to_string()),
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_1/key2",
                Value::String("value2".to_string()),
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_2/key3",
                Value::String("value3".to_string()),
            )
            .await?;

        // Test deleting a path with its contents
        store.delete_recursive("/danube/topics/topic_1").await?;

        // Assert that keys under the deleted path are gone
        assert!(store.get("/danube/topics/topic_1/key1").await.is_err());
        assert!(store.get("/danube/topics/topic_1/key2").await.is_err());

        // Assert that other directory and its key remain
        assert!(store.get("/danube/topics/topic_2/key3").await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_childrens() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = LocalMemoryMetadataStore::new();

        // Create a sample data structure
        store
            .put(
                "/danube/topics/topic_1/key1",
                Value::String("value1".to_string()),
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_2/key2",
                Value::String("value2".to_string()),
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_1/subtopic/key3",
                Value::String("value3".to_string()),
            )
            .await?;
        store
            .put("/data/other/path", Value::String("value4".to_string()))
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
