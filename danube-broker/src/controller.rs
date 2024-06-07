mod leader_election;
mod load_balance;
mod local_cache;
mod syncronizer;
pub(crate) use leader_election::{LeaderElection, LeaderElectionState};
pub(crate) use local_cache::LocalCache;

use anyhow::Result;
use load_balance::LoadBalance;
use std::sync::Arc;
use syncronizer::Syncronizer;
use tokio::sync::Mutex;

use crate::{broker_service::BrokerService, metadata_store::MetadataStorage, namespace::NameSpace};

static LEADER_SELECTION_PATH: &str = "/broker/leader";

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
// Load Balance:
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
    broker_id: u64,
    broker: Arc<Mutex<BrokerService>>,
    store: MetadataStorage,
    local_cache: LocalCache,
    leader_election_service: Option<LeaderElection>,
    syncronizer: Option<Syncronizer>,
    load_balance: LoadBalance,
}

impl Controller {
    pub(crate) fn new(
        broker_id: u64,
        broker: Arc<Mutex<BrokerService>>,
        local_cache: LocalCache,
        store: MetadataStorage,
    ) -> Self {
        Controller {
            broker_id,
            broker,
            store,
            local_cache,
            leader_election_service: None,
            syncronizer: None,
            load_balance: LoadBalance::new(),
        }
    }
    pub(crate) async fn start(&self) -> Result<()> {
        let leader_election_service =
            LeaderElection::new(self.store.clone(), LEADER_SELECTION_PATH, self.broker_id);

        Ok(())
    }

    // Checks whether the broker owns a specific topic
    pub(crate) fn check_topic_ownership(&self, topic_name: &str) -> bool {
        self.load_balance
            .check_ownership(self.broker_id, topic_name)
    }

    // Lookup service for resolving topic names to their corresponding broker service URLs
    pub(crate) async fn get_broker_service_url(topic_name: &str) -> Result<LookupResult> {
        todo!();
    }
}

pub(crate) enum LookupResult {
    BrokerUrl(String),
    RedirectUrl(String),
}
