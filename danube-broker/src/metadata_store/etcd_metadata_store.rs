use anyhow::Result;
use etcd_client::{Client, GetOptions, PutOptions};
use serde_json::Value;
use std::error::Error;

use crate::metadata_store::{MetaOptions, MetadataStore};

use super::MetadataStoreConfig;

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
    async fn get(&mut self, path: &str, get_options: MetaOptions) -> Result<Value> {
        let mut value: Value = serde_json::from_str("")?;
        let options = if let MetaOptions::EtcdGet(etcd_options) = get_options {
            Some(etcd_options)
        } else {
            None
        };

        let resp = self.client.get(path, options).await?;

        // Parse response into Value
        if let Some(kv) = resp.kvs().first() {
            value = serde_json::from_str(kv.value_str()?)?;
        }
        Ok(value)
    }

    async fn get_childrens(&mut self, path: &str) -> Result<Vec<String>> {
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

    async fn put(&mut self, path: &str, value: Value, put_options: MetaOptions) -> Result<()> {
        let mut kv_client = self.client.kv_client();
        let options = if let MetaOptions::EtcdPut(etcd_options) = put_options {
            Some(etcd_options)
        } else {
            None
        };

        let value_str = serde_json::to_string(&value)?;
        kv_client.put(path, value_str, options).await?;

        Ok(())
    }

    async fn delete(&mut self, path: &str) -> Result<Option<Value>> {
        let mut kv_client = self.client.kv_client();

        let response = kv_client.delete(path, None).await?;

        let value: Value = serde_json::from_str(response.deleted().to_string().as_str())?;

        Ok(Some(value))
    }

    async fn delete_recursive(&mut self, path: &str) -> Result<()> {
        let mut kv_client = self.client.kv_client();

        let response = kv_client.delete(path, None).await?;

        let _: Value = serde_json::from_str(response.deleted().to_string().as_str())?;

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
