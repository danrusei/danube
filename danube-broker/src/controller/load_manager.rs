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

#[derive(Debug)]
pub(crate) struct LoadManager {
    // handle to the Metadata Store
    store: MetadataStorage,
    // broker_id to LoadReport
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
    // rankings based on the load calculation
    rankings: Arc<Mutex<Vec<(u64, usize)>>>,
    // the broker_id to be served to the caller on function get_next_broker
    next_broker: AtomicU64,
}

// A Dummy implementation for the Load Manager, which has to be refined
impl LoadManager {
    pub fn new(broker_id: u64, store: MetadataStorage) -> Self {
        let mut brokers = Vec::new();
        brokers.push(broker_id);
        LoadManager {
            store,
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: AtomicU64::new(broker_id),
        }
    }

    pub(crate) async fn start(&mut self) -> Result<()> {
        // fetch the initial broker Load information
        self.fetch_initial_load().await;

        let brokers_usage_clone = Arc::clone(&self.brokers_usage);
        let rankings_clone = Arc::clone(&self.rankings);

        calculate_rankings_simple(&brokers_usage_clone, &rankings_clone);

        let (tx1, mut rx1) = mpsc::channel(32);

        let client = if let Some(client) = self.store.get_client() {
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
            etcd_watch_prefixes(client, prefixes, tx1).await;
        });

        let brokers_usage_clone = Arc::clone(&self.brokers_usage);
        let rankings_clone = Arc::clone(&self.rankings);

        // process the ETCD events
        tokio::spawn(async move {
            while let Some(event) = rx1.recv().await {
                process_event(event, &brokers_usage_clone);
                calculate_rankings_simple(&brokers_usage_clone, &rankings_clone);
            }
        });

        Ok(())
    }

    async fn fetch_initial_load(&self) {
        todo!()
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
}

async fn process_event(
    event: ETCDWatchEvent,
    brokers_usage: &Arc<Mutex<HashMap<u64, LoadReport>>>,
) {
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

    let mut brokers_usage = brokers_usage.lock().await;
    brokers_usage.insert(broker_id, load_report);
}

fn extract_broker_id(key: &str) -> Option<u64> {
    key.strip_prefix("/cluster/brokers/load/")?.parse().ok()
}

fn parse_load_report(value: &[u8]) -> Option<LoadReport> {
    let value_str = std::str::from_utf8(value).ok()?;
    serde_json::from_str(value_str).ok()
}

// Simple Load Calculation: the load is just based on the number of topics.
async fn calculate_rankings_simple(
    brokers_usage: &Arc<Mutex<HashMap<u64, LoadReport>>>,
    rankings: &Arc<Mutex<Vec<(u64, usize)>>>,
) {
    let brokers_usage = brokers_usage.lock().await;

    let mut broker_loads: Vec<(u64, usize)> = brokers_usage
        .iter()
        .map(|(&broker_id, load_report)| (broker_id, load_report.topics))
        .collect();

    broker_loads.sort_by_key(|&(_, load)| load);

    let mut rankings = rankings.lock().await;
    *rankings = broker_loads;
}

// Composite Load Calculation: the load is based on the number of topics, CPU usage, and memory usage.
async fn calculate_rankings_composite(
    brokers_usage: Arc<Mutex<HashMap<u64, LoadReport>>>,
    rankings: Arc<Mutex<Vec<(u64, usize)>>>,
) {
    let brokers_usage = brokers_usage.lock().await;

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

    let mut rankings = rankings.lock().await;
    *rankings = broker_loads;
}
