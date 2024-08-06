use anyhow::Result;
use etcd_client::{Client, DeleteOptions, GetOptions};
use serde_json::Value;

use super::MetadataStoreConfig;
use crate::metadata_store::{MetaOptions, MetadataStore};

#[derive(Clone)]
pub(crate) struct EtcdMetadataStore {
    client: Client,
}

impl EtcdMetadataStore {
    pub async fn new(etcd_addr: String, _store_config: MetadataStoreConfig) -> Result<Self> {
        let client = Client::connect([etcd_addr], None).await?;
        Ok(EtcdMetadataStore { client })
    }
}

impl MetadataStore for EtcdMetadataStore {
    async fn get(&mut self, path: &str, get_options: MetaOptions) -> Result<Option<Value>> {
        // Extract ETCD options if provided
        let options = if let MetaOptions::EtcdGet(etcd_options) = get_options {
            Some(etcd_options)
        } else {
            None
        };

        // Fetch the data from ETCD
        let resp = self.client.get(path, options).await?;

        if let Some(kv) = resp.kvs().first() {
            // Deserialize the byte array into a serde_json::Value
            let value = serde_json::from_slice(kv.value())?;
            Ok(Some(value))
        } else {
            // Return None if no key-value pair was found
            Ok(None)
        }
    }

    async fn get_childrens(&mut self, path: &str) -> Result<Vec<String>> {
        let mut kv_client = self.client.kv_client();

        // Retrieve all keys with the specified prefix
        let range_resp = kv_client
            .get(path, Some(GetOptions::new().with_keys_only().with_prefix()))
            .await?;

        let mut child_paths: Vec<String> = Vec::new();

        for kv in range_resp.kvs() {
            let key = kv.key_str()?.to_owned();
            child_paths.push(key);
        }

        Ok(child_paths)
    }

    async fn put(&mut self, path: &str, value: Value, put_options: MetaOptions) -> Result<()> {
        let mut kv_client = self.client.kv_client();
        let options = if let MetaOptions::EtcdPut(etcd_options) = put_options {
            Some(etcd_options)
        } else {
            None
        };

        // Serialize serde_json::Value to a byte array
        let value_bytes = serde_json::to_vec(&value)?;
        kv_client.put(path, value_bytes, options).await?;

        Ok(())
    }

    async fn delete(&mut self, path: &str) -> Result<Option<Value>> {
        let mut kv_client = self.client.kv_client();

        // Set the DeleteOptions to return previous key-value pairs
        let delete_options = DeleteOptions::new().with_prev_key();

        let response = kv_client.delete(path, Some(delete_options)).await?;

        // Check if previous key-value pairs exist
        let value = if let Some(kv) = response.prev_kvs().first() {
            // Deserialize the byte array into a serde_json::Value
            Some(serde_json::from_slice(kv.value())?)
        } else {
            None
        };

        Ok(value)
    }

    // Deletes all keys that are prefixed with the specified path.
    // For example, if path is /foo, it will delete keys like /foo/bar, /foo/baz
    // this doesn't work, as with_from_key or with_prefix are not working as intended
    async fn delete_recursive(&mut self, path: &str) -> Result<()> {
        let mut kv_client = self.client.kv_client();

        // Set DeleteOptions to delete all keys with the specified prefix
        let delete_options = DeleteOptions::new().with_prefix();

        // Perform the delete operation
        kv_client.delete(path, Some(delete_options)).await?;

        Ok(())
    }

    fn get_client(&mut self) -> Option<etcd_client::Client> {
        Some(self.client.clone())
    }
}

impl std::fmt::Debug for EtcdMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("EtcdMetadataStore")
            .field("Results", &"etcd client".to_owned())
            .finish()?;

        Ok(())
    }
}
