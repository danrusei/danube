use etcd_client::{Client, GetOptions};
use serde_json::Value;
use std::error::Error;

use crate::metadata_store::MetadataStore;

pub(crate) struct EtcdMetadataStore {
    client: Client,
}

impl std::fmt::Debug for EtcdMetadataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("EtcdMetadataStore")
            .field("Results", &"etcd client".to_owned())
            .finish()?;

        Ok(())
    }
}

impl EtcdMetadataStore {
    pub async fn new(etcd_addr: String) -> Result<Self, Box<dyn Error>> {
        let client = Client::connect([etcd_addr], None).await?;
        Ok(EtcdMetadataStore { client })
    }
}

impl MetadataStore for EtcdMetadataStore {
    async fn get(&mut self, path: &str) -> Result<Value, Box<dyn Error>> {
        let mut value: Value = serde_json::from_str("")?;

        let resp = self.client.get(path, None).await?;

        // Parse response into Value
        if let Some(kv) = resp.kvs().first() {
            value = serde_json::from_str(kv.value_str()?)?;
        }
        Ok(value)
    }

    async fn get_childrens(&mut self, path: &str) -> Result<Vec<String>, Box<dyn Error>> {
        let mut kv_client = self.client.kv_client();

        //let kv_client_prefix = KvClientPrefix::new(kv_client.clone(), path.into());

        // Retrieve all keys with the specified prefix
        let range_resp = kv_client
            .get(path, Some(GetOptions::new().with_keys_only()))
            .await?;

        let mut child_paths: Vec<String> = Vec::new();

        for kv in range_resp.kvs() {
            child_paths.push(kv.key_str()?.to_owned())
        }

        Ok(child_paths)
    }

    async fn put(&mut self, path: &str, value: Value) -> Result<(), Box<dyn Error>> {
        let mut kv_client = self.client.kv_client();

        let value_str = serde_json::to_string(&value)?;
        kv_client.put(path, value_str, None).await?;

        Ok(())
    }

    async fn delete(&mut self, path: &str) -> Result<Option<Value>, Box<dyn Error>> {
        let mut kv_client = self.client.kv_client();

        let response = kv_client.delete(path, None).await?;

        let value: Value = serde_json::from_str(response.deleted().to_string().as_str())?;

        Ok(Some(value))
    }

    async fn delete_recursive(&mut self, path: &str) -> Result<(), Box<dyn Error>> {
        let mut kv_client = self.client.kv_client();

        let response = kv_client.delete(path, None).await?;

        let _: Value = serde_json::from_str(response.deleted().to_string().as_str())?;

        Ok(())
    }
}
