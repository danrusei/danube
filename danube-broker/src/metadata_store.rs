mod etcd_metadata_store;
mod local_memory_metadata_store;

pub(crate) trait MetadataStore {
    fn create(metadata_url: String) -> Self;
    // Read the value of one key, identified by the path
    fn get(path: String) -> String;
    // Put a new value for a given key
    fn put(path: String, value: String) -> Result<(), Box<dyn std::error::Error>>;
    // Delete the key / value from the store
    fn delete(path: String) -> Result<(), Box<dyn std::error::Error>>;
    // Delete a key-value pair and all the children nodes.
    fn delete_recursive(path: String) -> Result<(), Box<dyn std::error::Error>>;
}
