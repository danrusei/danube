mod leader_election;
mod load_manager;
mod local_cache;
mod syncronizer;
use etcd_client::PutOptions;
pub(crate) use leader_election::{LeaderElection, LeaderElectionState};
pub(crate) use load_manager::load_report::{generate_load_report, LoadReport};
pub(crate) use load_manager::LoadManager;
pub(crate) use local_cache::LocalCache;
pub(crate) use syncronizer::Syncronizer;

use anyhow::{anyhow, Result};
use core::sync;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, Duration};

use crate::metadata_store::{MetaOptions, MetadataStore};
use crate::{
    broker_service::{self, BrokerService},
    metadata_store::MetadataStorage,
    namespace::NameSpace,
    resources::{join_path, BASE_BROKER_PATH},
};

pub(crate) static LEADER_SELECTION_PATH: &str = "/broker/leader";

// Controller has Cluster and local Broker management responsabilities
//
// Cluster Coordination & Metadata Management:
// Implements a heartbeat mechanism for brokers to signal their health status to the cluster
// Interact with the underlying metadata store (such as ETCD)
// to persist metadata information within the local_cache and ensure consistency across the cluster.
//
// Namespace Creation and Management:
// Allow users to create & delete namespaces within the cluster.
// This includes specifying policies and configurations specific to each namespace.
//
// LookUp Service:
// Provide a mechanism for clients to discover the brokers that own the desired topics.
// This is essential for producers to know where to send messages and for consumers to know where to fetch messages from.
// Handle client redirections if the broker ownership of a topic or partition changes.
//
// Leader Election:
// Leader Election service is needed for critical tasks such as topic assignment to brokers or partitioning.
//
// Load Manager/Balance:
// Monitor and distribute load across brokers by managing topic and partitions assignments to brokers
// Implement rebalancing logic to redistribute topics/partitions when brokers join or leave the cluster.
// Responsible of the failover mechanisms to handle broker failures
//
// Monitoring and Metrics:
// Collect and provide metrics related to namespace usage, such as message rates, storage usage, and throughput.
// Integrate with monitoring and alerting systems to provide insights into namespace performance and detect anomalies.
//
// Resource Quotas:
// Implement and enforce resource quotas to ensure fair usage of resources among different namespaces.
// This includes limiting the number of topics, message rates, and storage usage.
#[derive(Debug)]
pub(crate) struct Controller {
    broker_id: Option<u64>,
    broker: Arc<Mutex<BrokerService>>,
    meta_store: MetadataStorage,
    local_cache: LocalCache,
    leader_election: LeaderElection,
    syncronizer: Syncronizer,
    load_manager: LoadManager,
}

impl Controller {
    pub(crate) fn new(
        broker: Arc<Mutex<BrokerService>>,
        meta_store: MetadataStorage,
        local_cache: LocalCache,
        leader_election: LeaderElection,
        syncronizer: Syncronizer,
        load_manager: LoadManager,
    ) -> Self {
        Controller {
            broker_id: None,
            broker,
            meta_store,
            local_cache,
            leader_election,
            syncronizer,
            load_manager,
        }
    }
    pub(crate) async fn start(&mut self) -> Result<()> {
        let broker_id = self.broker.lock().await.broker_id;
        self.broker_id = Some(broker_id);

        // Start the Syncronizer
        //==========================================================================

        // let syncronyzer = if let Ok(syncronizer) = Syncronizer::new() {
        //     syncronizer
        // } else {
        //     return Err(anyhow!("Unable to instantiate the Syncronizer"));
        // };

        // self.syncronizer = Some(syncronyzer);

        // Start the Leader Election Service
        //==========================================================================

        self.leader_election.start();

        // Start the Local Cache
        //==========================================================================

        // let local_cache = LocalCache::new();
        // Fetch initial data, populate cache & watch for Events to update local cache
        let mut client = self.meta_store.get_client();
        if let Some(client) = client {
            let local_cache_cloned = self.local_cache.clone();
            let rx_event = self.local_cache.populate_start_local_cache(client).await?;
            // Process the ETCD Watch events
            tokio::spawn(async move { local_cache_cloned.process_event(rx_event).await });
        }

        // Start the Load Manager Service
        //==========================================================================
        // at this point the broker will become visible to the rest of the brokers
        // by creating the registration and also

        // self.load_manager = Some(LoadManager::new(broker_id));

        let rx_event = self
            .load_manager
            .bootstrap(broker_id, self.meta_store.clone())
            .await?;

        let mut load_manager_cloned = self.load_manager.clone();
        //let broker_id_cloned = self.broker_id;

        // Process the ETCD Watch events
        tokio::spawn(async move { load_manager_cloned.start(rx_event, broker_id).await });

        let broker_service_cloned = Arc::clone(&self.broker);
        let meta_store_cloned = self.meta_store.clone();

        // Publish periodic Load Reports
        tokio::spawn(
            async move { post_broker_load_report(broker_service_cloned, meta_store_cloned) },
        );

        Ok(())
    }

    // Checks whether the broker owns a specific topic
    pub(crate) async fn check_topic_ownership(&self, topic_name: &str) -> bool {
        if let Some(broker_id) = self.broker_id {
            return self
                .load_manager
                .check_ownership(broker_id, topic_name)
                .await;
        }
        false
    }

    // Lookup service for resolving topic names to their corresponding broker service URLs
    pub(crate) async fn get_broker_service_url(topic_name: &str) -> Result<LookupResult> {
        todo!();
    }
}

async fn post_broker_load_report(
    broker_service: Arc<Mutex<BrokerService>>,
    mut meta_store: MetadataStorage,
) {
    let mut topics: Vec<String>;
    let mut broker_id;
    let mut interval = time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        {
            let broker_service = broker_service.lock().await;
            topics = broker_service.get_topics().into_iter().cloned().collect();
            broker_id = broker_service.broker_id;
        }
        let topics_len = topics.len();
        let load_repot: LoadReport = generate_load_report(topics_len, topics);
        if let Ok(value) = serde_json::to_value(load_repot) {
            let path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string()]);
            meta_store.put(&path, value, MetaOptions::None);
        }
    }
}

pub(crate) enum LookupResult {
    BrokerUrl(String),
    RedirectUrl(String),
}
