use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    errors::{MetadataError, Result},
    store::MetadataStore,
    watch::WatchStream,
};
use async_trait::async_trait;
use etcd_client::{Client, WatchOptions};

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
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut client = self.client.lock().await;
        let response = client.get(key, None).await.map_err(MetadataError::from)?;
        if let Some(kv) = response.kvs().first() {
            Ok(Some(kv.value().to_vec()))
        } else {
            Ok(None)
        }
    }

    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let mut client = self.client.lock().await;
        client
            .put(key, value, None)
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

impl Debug for EtcdStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdStore").finish()
    }
}
