use dashmap::mapref::one::RefMut;
use dashmap::DashMap;
use serde_json::Value;
use std::collections::BTreeMap;
use std::error::Error;

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

    // Read the value of one key, identified by the path
    fn get(&self, path: &str) -> Result<Value, Box<dyn Error>> {
        let bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        match bmap.get(&key) {
            Some(value) => Ok(value.clone()),
            None => Err(From::from("Key not found")),
        }
    }

    fn put(&mut self, path: &str, value: Value) -> Result<(), Box<dyn Error>> {
        let mut bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        if key == "" {
            return Err(From::from("wrong path"));
        }

        bmap.insert(key, value);

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_nonexistent_key() {
        let store = LocalMemoryMetadataStore::new();
        let topic_id = "unknown-topic";

        // Try to get a non-existent key
        let result = store.get(format!("/danube/topics/{}/metrics", topic_id).as_str());
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().to_string(), "Key not found");
    }

    #[test]
    fn test_put_and_get() {
        let mut store = LocalMemoryMetadataStore::new();
        let topic_id = "another-topic";
        let value: Value = serde_json::from_str("{\"sampling_rate\": 0.5}").unwrap();

        // Put a new value
        store
            .put(
                format!("/danube/topics/{}/conf", topic_id).as_str(),
                value.clone(),
            )
            .unwrap();

        // Get the value
        let retrieved_value = store
            .get(format!("/danube/topics/{}/conf", topic_id).as_str())
            .unwrap();
        assert_eq!(retrieved_value, value);
    }

    #[test]
    fn test_put_invalid_path() {
        let mut store = LocalMemoryMetadataStore::new();
        let value: Value = serde_json::from_str("{\"sampling_rate\": 0.5}").unwrap();

        // Try to put with invalid path (missing segment)
        let result = store.put("/danube/topics", value.clone());
        assert!(result.is_err());
    }
}
