pub(crate) mod load_report;
mod rankings;

use anyhow::{anyhow, Result};
use etcd_client::{Client, EventType, GetOptions};
use load_report::{LoadReport, ResourceType};
use rankings::{rankings_composite, rankings_simple};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, trace, warn};

use crate::{
    metadata_store::{
        etcd_watch_prefixes, ETCDWatchEvent, MetaOptions, MetadataStorage, MetadataStore,
    },
    resources::{
        BASE_BROKER_LOAD_PATH, BASE_BROKER_PATH, BASE_NAMESPACES_PATH, BASE_REGISTER_PATH,
        BASE_UNASSIGNED_PATH,
    },
    utils::join_path,
};

use super::{LeaderElection, LeaderElectionState};

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
    meta_store: MetadataStorage,
}

impl LoadManager {
    pub fn new(broker_id: u64, meta_store: MetadataStorage) -> Self {
        LoadManager {
            brokers_usage: Arc::new(Mutex::new(HashMap::new())),
            rankings: Arc::new(Mutex::new(Vec::new())),
            next_broker: Arc::new(AtomicU64::new(broker_id)),
            meta_store,
        }
    }
    pub async fn bootstrap(&mut self, _broker_id: u64) -> Result<mpsc::Receiver<ETCDWatchEvent>> {
        let client = if let Some(client) = self.meta_store.get_client() {
            client
        } else {
            return Err(anyhow!(
                "The Load Manager was unable to fetch the Metadata Store client"
            ));
        };

        // fetch the initial broker Load information
        let _ = self.fetch_initial_load(client.clone()).await;

        //calculate rankings after the initial load fetch
        self.calculate_rankings_simple().await;

        let (tx_event, rx_event) = mpsc::channel(32);

        // Watch for Metadata Store events (ETCD), interested of :
        //
        // "/cluster/load" to retrieve the load data for each broker from the cluster
        // with the data it calculates the rankings and post the informations on "/cluster/load_balance"
        //
        //
        // "/cluster/unassigned" for new created topics that due to be allocated to brokers
        // it using the calculated rankings to decide which broker will host the new topic
        // and inform the broker by posting the topic on it's path "/cluster/brokers/{broker-id}/{namespace}/{topic}"
        tokio::spawn(async move {
            let mut prefixes = Vec::new();
            prefixes.push(BASE_BROKER_LOAD_PATH.to_string());
            prefixes.push(BASE_UNASSIGNED_PATH.to_string());
            prefixes.push(BASE_REGISTER_PATH.to_string());
            let _ = etcd_watch_prefixes(client, prefixes, tx_event).await;
        });

        Ok(rx_event)
    }

    pub(crate) async fn fetch_initial_load(&self, mut client: Client) -> Result<()> {
        // Prepare the etcd request to get all keys under /cluster/brokers/load/
        let options = GetOptions::new().with_prefix();
        let response = client
            .get(BASE_BROKER_LOAD_PATH, Some(options))
            .await
            .expect("Failed to fetch keys from etcd");

        let mut brokers_usage = self.brokers_usage.lock().await;

        // Iterate through the key-value pairs in the response
        for kv in response.kvs() {
            let key = kv.key_str().expect("Failed to parse key");
            let value = kv.value();

            // Extract broker-id from key
            if let Some(broker_id_str) = key.strip_prefix(BASE_BROKER_LOAD_PATH) {
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

    pub(crate) async fn start(
        &mut self,
        mut rx_event: mpsc::Receiver<ETCDWatchEvent>,
        broker_id: u64,
        leader_election: LeaderElection,
    ) {
        while let Some(event) = rx_event.recv().await {
            let state = leader_election.get_state().await;

            match event.key.as_str() {
                key if key.starts_with(BASE_UNASSIGNED_PATH) => {
                    // only the Leader Broker should assign the topic
                    if state == LeaderElectionState::Following {
                        continue;
                    }
                    info!(
                        "Attempting to assign the new topic {} to a broker",
                        &event.key
                    );
                    self.assign_topic_to_broker(event).await;
                }
                key if key.starts_with(BASE_REGISTER_PATH) => {
                    // only the Leader Broker should realocate the cluster resources
                    if state == LeaderElectionState::Following {
                        continue;
                    }

                    if event.event_type == EventType::Delete {
                        let remove_broker = match key.split('/').last().unwrap().parse::<u64>() {
                            Ok(id) => id,
                            Err(err) => {
                                error!("Unable to parse the broker id: {}", err);
                                continue;
                            }
                        };
                        info!("Broker {} is no longer alive", remove_broker);
                        {
                            let mut brokers_usage_lock = self.brokers_usage.lock().await;
                            brokers_usage_lock.remove(&remove_broker);
                        }

                        {
                            let mut rankings_lock = self.rankings.lock().await;
                            rankings_lock.retain(|&(entry_id, _)| entry_id != remove_broker);
                        }

                        // TODO! - reallocate the resources to another broker
                        // for now I will delete from metadata store all the topics assigned to /cluster/brokers/removed_broker/*
                        match  self.delete_topic_allocation(remove_broker).await {
                        Ok(_) => {},
                        Err(err) => error!("Unable to delete resources of the unregistered broker {}, due to error {}", remove_broker, err)
                       }
                    }
                }

                key if key.starts_with(BASE_BROKER_LOAD_PATH) => {
                    // the event is processed and added localy,
                    // but only the Leader Broker does the calculations on the loads
                    trace!("A new load report has been received from: {}", &event.key);
                    self.process_event(event).await;

                    if state == LeaderElectionState::Following {
                        continue;
                    }
                    self.calculate_rankings_simple().await;
                    let next_broker = self
                        .rankings
                        .lock()
                        .await
                        .get(0)
                        .get_or_insert(&(broker_id, 0))
                        .0;
                    let _ = self
                        .next_broker
                        .swap(next_broker, std::sync::atomic::Ordering::SeqCst);

                    // need to post it's decision on the Metadata Store
                }
                _ => {
                    // Handle unexpected events if needed
                    error!("Received an unexpected event: {}", &event.key);
                }
            }
        }
    }

    // Post the topic on the broker address /cluster/brokers/{broker-id}/{namespace}/{topic}
    // to be further read and processed by the selected broker
    async fn assign_topic_to_broker(&mut self, event: ETCDWatchEvent) {
        if event.event_type != EventType::Put {
            return;
        }

        // recalculate the rankings after the topic was assigned, as the load report events comes on fix intervals
        self.calculate_rankings_simple().await;

        let parts: Vec<_> = event.key.split(BASE_UNASSIGNED_PATH).collect();
        let topic_name = parts[1];

        let broker_id = self.get_next_broker().await;
        let path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string(), topic_name]);

        match self
            .meta_store
            .put(&path, serde_json::Value::Null, MetaOptions::None)
            .await
        {
            Ok(_) => info!(
                "The topic {} was successfully assign to broker {}",
                topic_name, broker_id
            ),
            Err(err) => warn!(
                "Unable to assign topic {} to the broker {}, due to error: {}",
                topic_name, broker_id, err
            ),
        }

        // once the broker was notified, update internaly as well
        // in order to use somehow more accurate information until the new load reports are received
        let mut brokers_usage = self.brokers_usage.lock().await;
        if let Some(load_report) = brokers_usage.get_mut(&broker_id) {
            load_report.topics_len += 1;
            load_report.topic_list.push(topic_name.to_string());
        }
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
        let rankings = self.rankings.lock().await;
        let next_broker = rankings.get(0).unwrap().0;

        let _ = self
            .next_broker
            .swap(next_broker, std::sync::atomic::Ordering::SeqCst);

        next_broker
    }

    pub async fn delete_topic_allocation(&mut self, broker_id: u64) -> Result<()> {
        let path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string()]);
        let childrens = self.meta_store.get_childrens(&path).await?;

        // should delete the paths like this /cluster/brokers/6719542140305601108/namespace_name/topic_name
        // so the topics are not allocated with unregistered broker
        for children in childrens {
            self.meta_store.delete(&children).await?;

            // delete the topic from the /namespace path as well
            let parts: Vec<&str> = children.split('/').collect();
            let path_ns = join_path(&[
                BASE_NAMESPACES_PATH,
                &parts[4],
                "topics",
                &parts[4],
                &parts[5],
            ]);
            self.meta_store.delete(&path_ns).await?;
        }

        Ok(())
    }

    #[allow(dead_code)]
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
        let broker_loads = rankings_simple(self.brokers_usage.clone()).await;
        *self.rankings.lock().await = broker_loads;
    }

    // Composite Load Calculation: the load is based on the number of topics, CPU usage, and memory usage.
    #[allow(dead_code)]
    async fn calculate_rankings_composite(&self) {
        let broker_loads = rankings_composite(self.brokers_usage.clone()).await;
        *self.rankings.lock().await = broker_loads;
    }
}

fn extract_broker_id(key: &str) -> Option<u64> {
    key.strip_prefix(format!("{}/", BASE_BROKER_LOAD_PATH).as_str())?
        .parse()
        .ok()
}

fn parse_load_report(value: &[u8]) -> Option<LoadReport> {
    let value_str = std::str::from_utf8(value).ok()?;
    serde_json::from_str(value_str).ok()
}
