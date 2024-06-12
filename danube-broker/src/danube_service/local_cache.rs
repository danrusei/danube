use anyhow::Result;
use dashmap::DashMap;
use etcd_client::{Client, WatchOptions};
use futures::StreamExt;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::info;

use crate::metadata_store::{etcd_watch_prefixes, ETCDWatchEvent};
use crate::resources::{
    BASE_CLUSTERS_PATH, BASE_NAMESPACE_PATH, BASE_PRODUCER_PATH, BASE_SUBSCRIPTION_PATH,
    BASE_TOPIC_PATH,
};

// It caches various types of metadata required by Danube brokers, such as topic and namespace data,
// which are frequently accessed during message production and consumption.
// This reduces the need for frequent queries to the central metadata store: ETCD.
//
// The resources/_resources.md document describe how the resources are organized in Metadata Store
//
// By updating the local cache based on the events,
// brokers ensure they have the latest metadata without repeatedly querying the central store.
//
// The updates/events are received via the metadata event synchronizer and/or the Watch events.
//
// Note: The instance can be safety Cloned as all it's fields ar wrapped in Arc<>,
// allowing the LocalCache struct to be cloned without deep copying the underlying data.
#[derive(Debug, Clone)]
pub(crate) struct LocalCache {
    // holds information about the cluster and the cluster's brokers
    pub(crate) cluster: Arc<DashMap<String, (i64, Value)>>,
    // holds information about the namespace policy and the namespace's topics
    pub(crate) namespaces: Arc<DashMap<String, (i64, Value)>>,
    // holds information about the topic policy and topic metadata, including partitioned topics
    pub(crate) topics: Arc<DashMap<String, (i64, Value)>>,
    // holds information about the topic subscriptions, including their consumers
    pub(crate) subscriptions: Arc<DashMap<String, (i64, Value)>>,
    // holds information about the producers
    pub(crate) producers: Arc<DashMap<String, (i64, Value)>>,
}

impl LocalCache {
    pub(crate) fn new() -> Self {
        LocalCache {
            cluster: Arc::new(DashMap::new()),
            namespaces: Arc::new(DashMap::new()),
            topics: Arc::new(DashMap::new()),
            subscriptions: Arc::new(DashMap::new()),
            producers: Arc::new(DashMap::new()),
        }
    }

    pub(crate) fn update_cache(&self, key: &str, version: i64, value: Option<&[u8]>) {
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() < 2 {
            return;
        }

        let category = parts[1];
        let cache = match category {
            "namespace" => &self.namespaces,
            "topic" => &self.topics,
            "consumer" => &self.subscriptions,
            "producer" => &self.producers,
            _ => return,
        };

        if cache.contains_key(key) {
            if cache.get(key).unwrap().0 >= version {
                return;
            }
        }

        if let Some(value) = value {
            if let Ok(json_value) = serde_json::from_slice(value) {
                cache.insert(key.to_string(), (version, json_value));
            }
        } else {
            cache.remove(key);
        }
    }

    // Function to populate cache with initial data from etcd
    pub(crate) async fn populate_start_local_cache(
        &self,
        mut client: Client,
    ) -> Result<mpsc::Receiver<ETCDWatchEvent>> {
        let prefixes = vec!["/namespace", "/topic", "/consumer", "/producer"];

        for prefix in prefixes {
            let response = client
                .get(prefix, Some(etcd_client::GetOptions::new().with_prefix()))
                .await
                .unwrap();
            for kv in response.kvs() {
                let key = String::from_utf8(kv.key().to_vec()).unwrap();
                let value = kv.value();
                let version = kv.version();
                self.update_cache(&key, version, Some(value));
            }
        }
        info!("Initial cache populated");

        let (tx_event, mut rx_event) = mpsc::channel(32);

        // watch for ETCD events
        tokio::spawn(async move {
            let mut prefixes = Vec::new();
            prefixes.extend(
                [
                    BASE_CLUSTERS_PATH,
                    BASE_NAMESPACE_PATH,
                    BASE_TOPIC_PATH,
                    BASE_SUBSCRIPTION_PATH,
                    BASE_PRODUCER_PATH,
                ]
                .iter(),
            );
            etcd_watch_prefixes(client, prefixes, tx_event).await;
        });

        Ok(rx_event)
    }

    pub(crate) async fn process_event(&self, mut rx_event: mpsc::Receiver<ETCDWatchEvent>) {
        while let Some(event) = rx_event.recv().await {
            match event.event_type {
                etcd_client::EventType::Put => {
                    self.update_cache(&event.key, event.version, event.value.as_deref())
                }
                etcd_client::EventType::Delete => {
                    self.update_cache(&event.key, event.version, None)
                }
                _ => {}
            }
        }
    }
    pub fn get(&self, path: &str) -> Option<Value> {
        // Split the path by '/' and collect the segments into a vector
        let segments: Vec<&str> = path.split('/').collect();

        // Ensure the path has at least two segments (e.g., "/cluster/{key}")
        if segments.len() < 2 {
            return None;
        }

        // Determine which DashMap to access based on the first segment
        match segments[1] {
            "cluster" => self.cluster.get(path).map(|entry| entry.value().1.clone()),
            "namespaces" => self
                .namespaces
                .get(path)
                .map(|entry| entry.value().1.clone()),
            "topics" => self.topics.get(path).map(|entry| entry.value().1.clone()),
            "subscriptions" => self
                .subscriptions
                .get(path)
                .map(|entry| entry.value().1.clone()),
            "producers" => self
                .producers
                .get(path)
                .map(|entry| entry.value().1.clone()),
            _ => None,
        }
    }
}
