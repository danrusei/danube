mod leader_election;
mod local_cache;
pub(crate) use leader_election::{LeaderElection, LeaderElectionState};

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{broker_service::BrokerService, metadata_store::MetadataStorage};

static LEADER_SELECTION_PATH: &str = "/broker/leader";

#[derive(Debug)]
pub(crate) struct Controller {
    broker: Arc<Mutex<BrokerService>>,
    store: MetadataStorage,
    leader_election_service: Option<LeaderElection>,
}

impl Controller {
    pub(crate) async fn new(broker: Arc<Mutex<BrokerService>>, store: MetadataStorage) -> Self {
        Controller {
            broker,
            store,
            leader_election_service: None,
        }
    }
    pub(crate) async fn start(&self) -> Result<()> {
        let mut broker = self.broker.lock().await;

        let leader_election_service =
            LeaderElection::new(self.store.clone(), LEADER_SELECTION_PATH, broker.broker_id);

        Ok(())
    }
}
