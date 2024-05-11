mod etcd_metadata_store;
mod local_memory_metadata_store;

use serde_json::Value;
use std::error::Error;

pub(crate) trait MetadataStore {
    // Read the value of one key, identified by the path
    fn get(&self, path: &str) -> Result<Value, Box<dyn Error>>;
    // Return all the paths that are children to the specific path.
    fn get_childrens(&mut self, path: &str) -> Result<Vec<String>, Box<dyn Error>>;
    // Put a new value for a given key
    fn put(&mut self, path: &str, value: Value) -> Result<(), Box<dyn Error>>;
    // Delete the key / value from the store
    fn delete(&mut self, path: &str) -> Result<Option<Value>, Box<dyn Error>>;
    // Delete a key-value pair and all the children nodes.
    fn delete_recursive(&mut self, path: &str) -> Result<(), Box<dyn Error>>;
}
