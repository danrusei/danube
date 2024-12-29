use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    errors::{MetadataError, Result},
    store::{MetaOptions, MetadataStore},
    watch::WatchStream,
};
use async_trait::async_trait;
use etcd_client::{Client, GetOptions, LeaseGrantResponse, PutOptions, WatchOptions};
use serde_json::Value;

#[derive(Clone)]
pub struct EtcdStore {
    client: Arc<Mutex<Client>>,
}

impl EtcdStore {
    pub async fn new(endpoints: Vec<String>) -> Result<Self> {
        let client = Client::connect(endpoints, None)
            .await
            .map_err(MetadataError::from)?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }
}

#[async_trait]
impl MetadataStore for EtcdStore {
    async fn get(&self, key: &str, get_options: MetaOptions) -> Result<Option<Value>> {
        // Extract ETCD options if provided
        let options = if let MetaOptions::EtcdGet(etcd_options) = get_options {
            Some(etcd_options)
        } else {
            None
        };

        let mut client = self.client.lock().await;
        let response = client
            .get(key, options)
            .await
            .map_err(MetadataError::from)?;
        if let Some(kv) = response.kvs().first() {
            let value = serde_json::from_slice(kv.value())?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    async fn get_childrens(&self, path: &str) -> Result<Vec<String>> {
        let client = self.client.lock().await;
        let mut kv_client = client.kv_client();

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

    async fn put(&self, key: &str, value: Value, put_options: MetaOptions) -> Result<()> {
        let options = if let MetaOptions::EtcdPut(etcd_options) = put_options {
            Some(etcd_options)
        } else {
            None
        };

        // Serialize serde_json::Value to a byte array
        let value_bytes = serde_json::to_vec(&value)?;

        let mut client = self.client.lock().await;
        client
            .put(key, value_bytes, options)
            .await
            .map_err(MetadataError::from)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut client = self.client.lock().await;
        client
            .delete(key, None)
            .await
            .map_err(MetadataError::from)?;
        Ok(())
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        let mut client = self.client.lock().await;
        let (_watcher, watch_stream) = client
            .watch(prefix, Some(WatchOptions::new().with_prefix()))
            .await
            .map_err(MetadataError::from)?;
        Ok(WatchStream::from_etcd(watch_stream))
    }
}

impl EtcdStore {
    pub async fn create_lease(&self, ttl: i64) -> Result<LeaseGrantResponse> {
        let mut client = self.client.lock().await;
        let lease = client.lease_grant(ttl, None).await?;
        Ok(lease)
    }

    pub async fn keep_lease_alive(&self, lease_id: i64) -> Result<()> {
        let mut client = self.client.lock().await;
        client.lease_keep_alive(lease_id).await?;
        Ok(())
    }

    pub async fn put_with_lease(&self, key: &str, value: Vec<u8>, lease_id: i64) -> Result<()> {
        let mut client = self.client.lock().await;
        let opts = PutOptions::new().with_lease(lease_id);
        client.put(key, value, Some(opts)).await?;
        Ok(())
    }
}

impl Debug for EtcdStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdStore").finish()
    }
}
