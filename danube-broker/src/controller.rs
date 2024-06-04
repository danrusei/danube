mod leader_election;

pub(crate) use leader_election::{LeaderElection, LeaderElectionState};

use crate::metadata_store::MetadataStorage;

#[derive(Debug)]
pub(crate) struct Controller {
    leader_election_service: LeaderElection,
    store: MetadataStorage,
}

impl Controller {
    pub(crate) fn new(broker_id: u64, store: MetadataStorage) -> Self {
        let path = "/broker/leader";
        let leader_election_service = LeaderElection::new(store.clone(), path, broker_id);
        Controller {
            leader_election_service,
            store,
        }
    }
}
