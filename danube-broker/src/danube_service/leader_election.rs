use crate::metadata_store::{MetaOptions, MetadataStorage, MetadataStore};
use anyhow::{anyhow, Result};
use etcd_client::{
    Client, Error, GetOptions as EtcdGetOptions, LeaseKeepAliveStream, PutOptions as EtcdPutOptions,
};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::time::{self, error::Elapsed, Duration, Interval};
use tracing::{info, trace, warn};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LeaderElectionState {
    NoLeader,
    Leading,
    Following,
}

// Leader Election service is needed for critical tasks such as topic assignment to brokers and partitioning.
// Load Manager is using this service, as only one broker is selected to make the load usage calculations and post the results.
// Should be selected one broker leader per cluster, who takes the decissions.
#[derive(Debug)]
pub(crate) struct LeaderElection {
    path: String,
    broker_id: u64,
    store: MetadataStorage,
    leader_check_interval: Interval,
    state: Arc<Mutex<LeaderElectionState>>,
}

impl LeaderElection {
    pub fn new(store: MetadataStorage, path: &str, broker_id: u64) -> Self {
        let leader_check_interval = time::interval(Duration::from_secs(10));

        Self {
            path: path.to_owned(),
            broker_id,
            store,
            leader_check_interval,
            state: Arc::new(Mutex::new(LeaderElectionState::NoLeader)),
        }
    }

    pub async fn start(&mut self) {
        //      self.elect().await;
        loop {
            self.check_leader().await;
            self.leader_check_interval.tick().await;
        }
    }

    pub fn get_state(&self) -> LeaderElectionState {
        let state = self.state.lock().unwrap();
        state.clone()
    }

    fn set_state(&self, new_state: LeaderElectionState) {
        let mut state = self.state.lock().unwrap();
        if *state != new_state {
            *state = new_state;
        }
    }

    async fn elect(&mut self) {
        info!("Broker {} attempting to become the leader", self.broker_id);
        match self.try_to_become_leader().await {
            Ok(is_leader) => {
                if is_leader {
                    self.set_state(LeaderElectionState::Leading);
                } else {
                    self.set_state(LeaderElectionState::Following);
                }
            }
            Err(e) => {
                warn!("Election error: {}", e);
            }
        }
    }

    async fn try_to_become_leader(&mut self) -> Result<bool> {
        let mut client = if let Some(client) = self.store.get_client() {
            client
        } else {
            return Err(anyhow!("unable to get the etcd_client"));
        };

        let payload = self.broker_id.clone();
        let lease_id = client.lease_grant(55, None).await?.id();
        let put_opts = EtcdPutOptions::new().with_lease(lease_id);

        let payload = serde_json::Value::Number(serde_json::Number::from(payload));

        match self
            .store
            .put(self.path.as_str(), payload, MetaOptions::EtcdPut(put_opts))
            .await
        {
            Ok(_) => {
                self.keep_alive_lease(lease_id).await?;
                Ok(true)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn keep_alive_lease(&mut self, lease_id: i64) -> Result<()> {
        let mut client = if let Some(client) = self.store.get_client() {
            client
        } else {
            return Err(anyhow!("unable to get the etcd_client"));
        };
        let (mut keeper, mut stream) = client.lease_keep_alive(lease_id).await?;

        tokio::spawn(async move {
            while let Some(_) = stream.message().await.unwrap_or(None) {
                info!("Lease {} renewed", lease_id);
            }
        });

        keeper.keep_alive().await?;
        Ok(())
    }

    async fn check_leader(&mut self) -> Result<()> {
        match self.store.get(self.path.as_str(), MetaOptions::None).await {
            Ok(response) => {
                if response.is_none() {
                    self.elect().await;
                } else {
                    let leader_id: u64 = response
                        .expect("checked aboved that the value is present")
                        .as_u64()
                        .expect("Broker Id should be a valid u64");
                    if leader_id == self.broker_id {
                        self.set_state(LeaderElectionState::Leading);
                        info!("Broker {} is the leader", self.broker_id);
                    } else {
                        self.set_state(LeaderElectionState::Following);
                        info!("Broker {} is a follower", self.broker_id);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to check leader: {}", e);
            }
        }
        Ok(())
    }
}
