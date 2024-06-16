pub(crate) mod load_report;
use load_report::{LoadReport, ResourceType, SystemLoad};

use anyhow::{anyhow, Result};
use etcd_client::{Client, EventType, GetOptions};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task;
use tokio::time::{self, Duration};

use crate::metadata_store::MetaOptions;
use crate::{
    metadata_store::{etcd_watch_prefixes, ETCDWatchEvent, MetadataStorage, MetadataStore},
    resources::{BASE_BROKER_PATH, LOADBALANCE_DECISION_PATH},
};

// The Load Manager monitors and distributes load across brokers by managing topic and partition assignments.
// It implements rebalancing logic to redistribute topics/partitions when brokers join or leave the cluster
// and is responsible for failover mechanisms to handle broker failures.
#[derive(Debug, Clone)]
pub(crate) struct LoadManager {
    // broker_id to LoadReport
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
    // rankings based on the load calculation
    rankings: Arc<Mutex<Vec<(u64, usize)>>>,
    // the broker_id to be served to the caller on function get_next_broker
    next_broker: Arc<AtomicU64>,
}

impl LoadManager {
    pub fn new(broker_id: u64) -> Self {
        LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(broker_id)),
        }
    }
    pub async fn bootstrap(
        &self,
        broker_id: u64,
        mut store: MetadataStorage,
    ) -> Result<mpsc::Receiver<ETCDWatchEvent>> {
        let client = if let Some(client) = store.get_client() {
            client
        } else {
            return Err(anyhow!(
                "The Load Manager was unable to fetch the Metadata Store client"
            ));
        };

        // fetch the initial broker Load information
        self.fetch_initial_load(client.clone()).await;

        //calculate rankings after the initial load fetch
        self.calculate_rankings_simple();

        let (tx_event, mut rx_event) = mpsc::channel(32);

        // watch for ETCD events
        tokio::spawn(async move {
            let mut prefixes = Vec::new();
            prefixes.push(BASE_BROKER_PATH);
            etcd_watch_prefixes(client, prefixes, tx_event).await;
        });

        Ok(rx_event)
    }

    pub(crate) async fn start(
        &mut self,
        mut rx_event: mpsc::Receiver<ETCDWatchEvent>,
        broker_id: u64,
    ) {
        while let Some(event) = rx_event.recv().await {
            self.process_event(event);
            self.calculate_rankings_simple();
            let next_broker = self
                .rankings
                .lock()
                .await
                .get(0)
                .get_or_insert(&(broker_id, 0))
                .0;
            let _ = self
                .next_broker
                .fetch_add(next_broker, std::sync::atomic::Ordering::SeqCst);

            // need to post it's decision on the Metadata Store
        }
    }

    pub(crate) async fn fetch_initial_load(&self, mut client: Client) -> Result<()> {
        // Prepare the etcd request to get all keys under /cluster/brokers/load/
        let options = GetOptions::new().with_prefix();
        let response = client
            .get("/cluster/brokers/load/", Some(options))
            .await
            .expect("Failed to fetch keys from etcd");

        let mut brokers_usage = self.brokers_usage.lock().await;

        // Iterate through the key-value pairs in the response
        for kv in response.kvs() {
            let key = kv.key_str().expect("Failed to parse key");
            let value = kv.value();

            // Extract broker-id from key
            if let Some(broker_id_str) = key.strip_prefix("/cluster/brokers/load/") {
                if let Ok(broker_id) = broker_id_str.parse::<u64>() {
                    // Deserialize the value to LoadReport
                    if let Ok(load_report) = serde_json::from_slice::<LoadReport>(value) {
                        brokers_usage.insert(broker_id, load_report);
                    } else {
                        return Err(anyhow!(
                            "Failed to deserialize LoadReport for broker_id: {}",
                            broker_id
                        ));
                    }
                } else {
                    return Err(anyhow!("Invalid broker_id format in key: {}", key));
                }
            } else {
                return Err(anyhow!("Key does not match expected pattern: {}", key));
            }
        }

        Ok(())
    }

    async fn process_event(&self, event: ETCDWatchEvent) {
        if event.event_type != EventType::Put {
            return;
        }

        let broker_id = match extract_broker_id(&event.key) {
            Some(id) => id,
            None => return,
        };

        let load_report = match event.value.as_deref().and_then(parse_load_report) {
            Some(report) => report,
            None => return,
        };

        let mut brokers_usage = self.brokers_usage.lock().await;
        brokers_usage.insert(broker_id, load_report);
    }

    pub async fn get_next_broker(&mut self) -> u64 {
        let first_in_list = self.rankings.lock().await.get(0).unwrap().0;

        let next_broker = self
            .next_broker
            .fetch_add(first_in_list, std::sync::atomic::Ordering::SeqCst);

        next_broker
    }

    pub(crate) async fn check_ownership(&self, broker_id: u64, topic_name: &str) -> bool {
        let brokers_usage = self.brokers_usage.lock().await;
        if let Some(load_report) = brokers_usage.get(&broker_id) {
            if load_report.topic_list.contains(&topic_name.to_owned()) {
                return true;
            }
        }
        false
    }

    // Simple Load Calculation: the load is just based on the number of topics.
    async fn calculate_rankings_simple(&self) {
        let brokers_usage = self.brokers_usage.lock().await;

        let mut broker_loads: Vec<(u64, usize)> = brokers_usage
            .iter()
            .map(|(&broker_id, load_report)| (broker_id, load_report.topics_len))
            .collect();

        broker_loads.sort_by_key(|&(_, load)| load);

        *self.rankings.lock().await = broker_loads;
    }

    // Composite Load Calculation: the load is based on the number of topics, CPU usage, and memory usage.
    async fn calculate_rankings_composite(&self) {
        let brokers_usage = self.brokers_usage.lock().await;

        let weights = (1.0, 0.5, 0.5); // (topics_weight, cpu_weight, memory_weight)

        let mut broker_loads: Vec<(u64, usize)> = brokers_usage
            .iter()
            .map(|(&broker_id, load_report)| {
                let topics_load = load_report.topics_len as f64 * weights.0;
                let mut cpu_load = 0.0;
                let mut memory_load = 0.0;

                for system_load in &load_report.resources_usage {
                    match system_load.resource {
                        ResourceType::CPU => cpu_load += system_load.usage as f64 * weights.1,
                        ResourceType::Memory => memory_load += system_load.usage as f64 * weights.2,
                    }
                }

                let total_load = (topics_load + cpu_load + memory_load) as usize;
                (broker_id, total_load)
            })
            .collect();

        broker_loads.sort_by_key(|&(_, load)| load);

        *self.rankings.lock().await = broker_loads;
    }
}

fn extract_broker_id(key: &str) -> Option<u64> {
    key.strip_prefix("/cluster/brokers/load/")?.parse().ok()
}

fn parse_load_report(value: &[u8]) -> Option<LoadReport> {
    let value_str = std::str::from_utf8(value).ok()?;
    serde_json::from_str(value_str).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_load_report(cpu_usage: usize, memory_usage: usize, topics_len: usize) -> LoadReport {
        LoadReport {
            resources_usage: vec![
                SystemLoad {
                    resource: ResourceType::CPU,
                    usage: cpu_usage,
                },
                SystemLoad {
                    resource: ResourceType::Memory,
                    usage: memory_usage,
                },
            ],
            topics_len,
            topic_list: vec!["/default/topic_name".to_string()],
        }
    }

    #[tokio::test]
    async fn test_single_broker() {
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
        };

        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(50, 30, 10));

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 1);
        assert_eq!(rankings[0], (1, 50)); // 10 * 1.0 + 50 * 0.5 + 30 * 0.5 = 75
    }

    #[tokio::test]
    async fn test_multiple_brokers() {
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
        };

        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(50, 30, 10));
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(2, create_load_report(20, 20, 5));
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(3, create_load_report(10, 10, 2));

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 3);
        assert_eq!(rankings[0], (3, 12)); // 2 * 1.0 + 10 * 0.5 + 10 * 0.5 = 12
        assert_eq!(rankings[1], (2, 25)); // 5 * 1.0 + 20 * 0.5 + 20 * 0.5 = 25
        assert_eq!(rankings[2], (1, 50)); // 10 * 1.0 + 50 * 0.5 + 30 * 0.5 = 75
    }

    #[tokio::test]
    async fn test_empty_brokers_usage() {
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
        };

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 0);
    }

    #[tokio::test]
    async fn test_same_load_brokers() {
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
        };

        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(1, create_load_report(10, 10, 5)); // Load: 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(2, create_load_report(10, 10, 5)); // Load: 15
        load_manager
            .brokers_usage
            .lock()
            .await
            .insert(3, create_load_report(10, 10, 5)); // Load: 15

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 3);
        assert!(rankings.contains(&(1, 15))); // 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
        assert!(rankings.contains(&(2, 15))); // 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
        assert!(rankings.contains(&(3, 15))); // 5 * 1.0 + 10 * 0.5 + 10 * 0.5 = 15
    }
}
