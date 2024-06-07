use anyhow::Result;
use dashmap::DashMap;
use etcd_client::{Client, WatchOptions};
use futures::StreamExt;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

// The LocalCache holds the local state of the metadata to enable quick access
// and reduce the need for frequent queries to the central metadata store: ETCD.
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
    pub(crate) cluster: Arc<DashMap<String, Value>>,
    // holds information about the namespace policy and the namespace's topics
    pub(crate) namespaces: Arc<DashMap<String, Value>>,
    // holds information about the topic policy and topic metadata, including partitioned topics
    pub(crate) topics: Arc<DashMap<String, Value>>,
    // holds information about the topic subscriptions, including their consumers
    pub(crate) subscriptions: Arc<DashMap<String, Value>>,
    // holds information about the producers
    pub(crate) producers: Arc<DashMap<String, Value>>,
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

    pub(crate) fn update_cache(&self, key: &str, value: Option<&[u8]>) {
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

        if let Some(value) = value {
            if let Ok(json_value) = serde_json::from_slice(value) {
                cache.insert(key.to_string(), json_value);
            }
        } else {
            cache.remove(key);
        }
    }
}

//TODO! Handling Conflicts:
// The implementation should include mechanisms to handle potential conflicts, such as version checks.
// The path/field version is already provided by ETCD
// For example, when updating or deleting metadata, the code checks the expected version
// to ensure that it is performing the action on the correct version of the metadata.

// Function to populate cache with initial data from etcd
pub(crate) async fn populate_cache(client: &mut Client, local_cache: &LocalCache) {
    let prefixes = vec!["/namespace", "/topic", "/consumer", "/producer"];

    for prefix in prefixes {
        let response = client
            .get(prefix, Some(etcd_client::GetOptions::new().with_prefix()))
            .await
            .unwrap();
        for kv in response.kvs() {
            let key = String::from_utf8(kv.key().to_vec()).unwrap();
            let value = kv.value();
            local_cache.update_cache(&key, Some(value));
        }
    }
    println!("Initial cache populated");
}

async fn watch_etcd(
    client: Client,
    local_cache: Arc<Mutex<LocalCache>>,
    prefixes: Vec<String>,
) -> Result<()> {
    for prefix in prefixes {
        let mut client_clone = client.clone();
        let cache_clone = Arc::clone(&local_cache);

        tokio::task::spawn(async move {
            if let Err(e) = async {
                let (watcher, mut watch_stream) = client_clone
                    .watch(prefix, Some(WatchOptions::new().with_prefix()))
                    .await?;

                while let Some(watch_response) = watch_stream.next().await {
                    for event in watch_response.expect("should be valid events").events() {
                        let key = event
                            .kv()
                            .expect("Todo")
                            .key_str()
                            .unwrap_or_default()
                            .to_string();
                        let value = event.kv().expect("Todo").value();
                        let cache_clone = Arc::clone(&cache_clone);
                        let mut cache = cache_clone.lock().await;
                        match event.event_type() {
                            etcd_client::EventType::Put => cache.update_cache(&key, Some(value)),
                            etcd_client::EventType::Delete => cache.update_cache(&key, None),
                            _ => {}
                        }
                    }
                }
                Ok::<(), etcd_client::Error>(())
            }
            .await
            {
                eprintln!("Error watching etcd: {:?}", e);
            }
        });
    }
    Ok(())
}

// // Initialize the etcd client
// let client = Client::connect(["http://localhost:2379"], None).await.unwrap();

// // Initialize the local cache
// let local_cache = LocalCache::new();

// // Fetch initial data and populate cache
// populate_cache(&client, &local_cache).await?;

// // Start watching for changes
// let prefixes = vec!["/prefix1".to_string(), "/prefix2".to_string()];
// watch_etcd(client, local_cache, prefixes).await?;
