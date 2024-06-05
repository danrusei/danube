use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::{
    broker_server,
    broker_service::{self, BrokerService},
    controller::{Controller, LeaderElection},
    metadata_store::{
        EtcdMetadataStore, MemoryMetadataStore, MetadataStorage, MetadataStoreConfig,
    },
    policies::Policies,
    resources::{Resources, DEFAULT_NAMESPACE},
    service_configuration::ServiceConfiguration,
    storage,
};

#[derive(Debug)]
pub(crate) struct DanubeService {
    config: ServiceConfiguration,
    resources: Option<Resources>,
    broker: Arc<Mutex<BrokerService>>,
    controller: Option<Controller>,
}

// DanubeService act as a a coordinator for managing clusters, including storage and brokers.
impl DanubeService {
    pub(crate) fn new(service_config: ServiceConfiguration) -> Self {
        let broker_service = BrokerService::new();
        let controller = None;

        DanubeService {
            config: service_config,
            resources: None,
            broker: Arc::new(Mutex::new(broker_service)),
            controller,
        }
    }

    pub(crate) async fn start(&mut self) -> Result<()> {
        let socket_addr = self.config.broker_addr.clone();

        let store_config = MetadataStoreConfig::new();
        let metadata_store: MetadataStorage =
            if let Some(etcd_addr) = self.config.meta_store_addr.clone() {
                MetadataStorage::EtcdStore(EtcdMetadataStore::new(etcd_addr, store_config).await?)
            } else {
                MetadataStorage::MemoryStore(MemoryMetadataStore::new(store_config).await?)
            };

        let mut resources = Resources::new(metadata_store);

        resources.cluster.create_cluster(
            &self.config.cluster_name,
            self.config.broker_addr.to_string(),
        );

        //create the default Namespace
        if !resources
            .namespace
            .namespace_exist(DEFAULT_NAMESPACE)
            .await?
        {
            let policies = Policies::new();
            resources
                .namespace
                .create_policies(DEFAULT_NAMESPACE, policies)
                .await?;
        } else {
            info!("Namespace {} already exists.", DEFAULT_NAMESPACE);
            // ensure that the policies are in place for the Default Namespace
            let _policies = resources.namespace.get_policies(DEFAULT_NAMESPACE).await?;
        }

        let storage = storage::memory_segment_storage::SegmentStore::new();

        let grpc_server =
            broker_server::DanubeServerImpl::new(self.broker.clone(), self.config.broker_addr);

        grpc_server.start().await?;

        //TODO! here we may want to start the MetadataEventSynchronizer & CoordinationService

        Ok(())
    }
}
