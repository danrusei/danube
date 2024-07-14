use super::{LoadReport, ResourceType};
//use LoadReport, ResourceType};

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) async fn rankings_simple(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
) -> Vec<(u64, usize)> {
    let brokers_usage = brokers_usage.lock().await;

    let mut broker_loads: Vec<(u64, usize)> = brokers_usage
        .iter()
        .map(|(&broker_id, load_report)| (broker_id, load_report.topics_len))
        .collect();

    broker_loads.sort_by_key(|&(_, load)| load);

    broker_loads
}

#[allow(dead_code)]
pub(crate) async fn rankings_composite(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
) -> Vec<(u64, usize)> {
    let brokers_usage = brokers_usage.lock().await;

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

    broker_loads
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::danube_service::load_manager::{
        load_report::SystemLoad, LoadManager, LoadReport, ResourceType,
    };
    use crate::metadata_store::{MemoryMetadataStore, MetadataStorage, MetadataStoreConfig};
    use std::sync::atomic::AtomicU64;

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
        let store_config = MetadataStoreConfig::new();
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::MemoryStore(
                MemoryMetadataStore::new(store_config)
                    .await
                    .expect("use for testing"),
            ),
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
        let store_config = MetadataStoreConfig::new();
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::MemoryStore(
                MemoryMetadataStore::new(store_config)
                    .await
                    .expect("use for testing"),
            ),
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
        let store_config = MetadataStoreConfig::new();
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::MemoryStore(
                MemoryMetadataStore::new(store_config)
                    .await
                    .expect("use for testing"),
            ),
        };

        load_manager.calculate_rankings_composite().await;

        let rankings = load_manager.rankings.lock().await;
        assert_eq!(rankings.len(), 0);
    }

    #[tokio::test]
    async fn test_same_load_brokers() {
        let store_config = MetadataStoreConfig::new();
        let load_manager = LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(0)),
            meta_store: MetadataStorage::MemoryStore(
                MemoryMetadataStore::new(store_config)
                    .await
                    .expect("use for testing"),
            ),
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
