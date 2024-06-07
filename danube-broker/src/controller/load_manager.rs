use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task;
use tokio::time::{self, Duration};

#[derive(Debug, Default)]
pub(crate) struct LoadManager {
    // the list of brokers
    brokers: Vec<u64>,
    // holds the assignmets of topics to brokers Key: broker_id , Value: topic_name (/{namespace}/{topic})
    brokers_topics: HashMap<u64, Vec<String>>,
    // broker_id to ResourceUsage
    brokers_usage: HashMap<u64, ResourceUsage>,
    // rankings based on the load calculation
    rankings: Vec<u64>,
    // the broker_id to be served to the caller on function get_next_broker
    next_broker: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum ResourceType {
    CPU,
    Memory,
}

#[derive(Debug, Clone)]
pub(crate) struct SystemLoad {
    resource: ResourceType,
    usage: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ResourceUsage {
    // current system usage of the broker
    system: Vec<SystemLoad>,
    // number of topics assigng to broker
    topics: usize,
}

// A Dummy implementation for the Load Manager, which has to be refined
impl LoadManager {
    pub fn new(broker_id: u64) -> Self {
        let mut load_manager = LoadManager::default();
        load_manager.brokers.push(broker_id);
        load_manager
    }

    pub(crate) async fn start(&mut self) {
        self.brokers = fetch_brokers();
        for &broker_id in &self.brokers {
            self.brokers_usage
                .insert(broker_id, fetch_resource_usage(broker_id));
        }
        self.calculate_rankings();
        self.next_broker = *self.rankings.first().unwrap_or(&0);

        let brokers_usage = Arc::new(Mutex::new(self.brokers_usage.clone()));
        let (tx, mut rx) = mpsc::channel(100);

        // Simulate listening for updates
        let brokers_usage_clone = Arc::clone(&brokers_usage);
        task::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                // Simulate receiving updates
                let updated_usage = fetch_resource_usage(1); // Dummy broker ID 1
                if tx.send((1, updated_usage)).await.is_err() {
                    break;
                }
            }
        });

        // Handle updates in a loop within start function
        while let Some((broker_id, usage)) = rx.recv().await {
            let mut brokers_usage = brokers_usage.lock().await;
            brokers_usage.insert(broker_id, usage);
            self.calculate_rankings();
        }
    }

    fn calculate_rankings(&mut self) {
        let mut brokers: Vec<_> = self.brokers_usage.iter().collect();
        brokers.sort_by_key(|&(_, usage)| usage.topics);
        self.rankings = brokers
            .into_iter()
            .map(|(&broker_id, _)| broker_id)
            .collect();
    }

    pub fn get_next_broker(&mut self) -> u64 {
        let next_broker = self.next_broker;
        self.next_broker = *self
            .rankings
            .iter()
            .cycle()
            .skip_while(|&&id| id != next_broker)
            .nth(1)
            .unwrap_or(&0);
        next_broker
    }

    pub(crate) fn check_ownership(&self, broker_id: u64, topic_name: &str) -> bool {
        if let Some(topics) = self.brokers_topics.get(&broker_id) {
            if topics.contains(&topic_name.to_owned()) {
                return true;
            }
        }
        false
    }
}

// Dummy functions for fetching brokers and resource usage
fn fetch_brokers() -> Vec<u64> {
    vec![1, 2, 3]
}

fn fetch_resource_usage(_broker_id: u64) -> ResourceUsage {
    ResourceUsage {
        system: vec![SystemLoad {
            resource: ResourceType::CPU,
            usage: 10,
        }],
        topics: 5,
    }
}
