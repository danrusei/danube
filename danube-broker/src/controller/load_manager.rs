use anyhow::{anyhow, Result};
use etcd_client::{Client, EventType};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task;
use tokio::time::{self, Duration};

use crate::{
    load_report::{LoadReport, ResourceType},
    metadata_store::{etcd_watch_prefixes, ETCDWatchEvent, MetadataStorage, MetadataStore},
    resources::{BASE_BROKER_PATH, LOADBALACE_DECISION_PATH},
};

// Load Manager, monitor and distribute load across brokers
// by managing topic and partitions assignments to brokers
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
        // fetch the initial broker Load information
        self.fetch_initial_load().await;

        //calculate rankings after the initial usage fetch
        self.calculate_rankings_simple();

        let (tx_event, mut rx_event) = mpsc::channel(32);

        let client = if let Some(client) = store.get_client() {
            client
        } else {
            return Err(anyhow!(
                "The Load Manager was unable to fetch the Metadata Store client"
            ));
        };

        // watch for ETCD events
        tokio::spawn(async move {
            let mut prefixes = Vec::new();
            prefixes.push(BASE_BROKER_PATH);
            etcd_watch_prefixes(client, prefixes, tx_event).await;
        });

        Ok(rx_event)
    }

    pub(crate) async fn start(&mut self, mut rx_event: mpsc::Receiver<ETCDWatchEvent>) {
        while let Some(event) = rx_event.recv().await {
            self.process_event(event);
            self.calculate_rankings_simple();
            // need to post it's decision on the Metadata Store
        }
    }

    async fn fetch_initial_load(&self) {
        todo!()
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
            .map(|(&broker_id, load_report)| (broker_id, load_report.topics))
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
                let topics_load = load_report.topics as f64 * weights.0;
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
    use crate::load_report::SystemLoad;

    fn create_load_report(cpu_usage: usize, memory_usage: usize, topics: usize) -> LoadReport {
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
            topics,
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
