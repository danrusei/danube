mod leader_election;
mod load_balance;
mod local_cache;
mod syncronizer;
pub(crate) use leader_election::{LeaderElection, LeaderElectionState};

use anyhow::Result;
use load_balance::LoadBalance;
use local_cache::LocalCache;
use std::sync::Arc;
use syncronizer::Syncronizer;
use tokio::sync::Mutex;

use crate::{broker_service::BrokerService, metadata_store::MetadataStorage, namespace::NameSpace};

static LEADER_SELECTION_PATH: &str = "/broker/leader";

#[derive(Debug)]
pub(crate) struct Controller {
    broker: Arc<Mutex<BrokerService>>,
    store: MetadataStorage,
    local_cache: LocalCache,
    leader_election_service: Option<LeaderElection>,
    syncronizer: Option<Syncronizer>,
    load_balance: LoadBalance,
}

impl Controller {
    pub(crate) fn new(broker: Arc<Mutex<BrokerService>>, store: MetadataStorage) -> Self {
        Controller {
            broker,
            store,
            local_cache: LocalCache::new(),
            leader_election_service: None,
            syncronizer: None,
            load_balance: LoadBalance::new(),
        }
    }
    pub(crate) async fn start(&self) -> Result<()> {
        let mut broker = self.broker.lock().await;

        let leader_election_service =
            LeaderElection::new(self.store.clone(), LEADER_SELECTION_PATH, broker.broker_id);

        Ok(())
    }

    // Checks whether the broker owns a specific topic
    pub(crate) fn check_topic_ownership(&self, topic_name: &str) -> Result<bool> {
        todo!()
    }
}
