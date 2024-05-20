use std::sync::Arc;

use crate::danube_server;
use crate::metadata_store::{EtcdMetadataStore, MemoryMetadataStore, MetadataStorage};

use crate::resources::DanubeResources;
use crate::service_configuration::ServiceConfiguration;
use crate::{broker_service::BrokerService, storage};

#[derive(Debug)]
pub(crate) struct DanubeService {
    config: ServiceConfiguration,
    resources: DanubeResources,
    broker: Arc<BrokerService>,
}

// DanubeService act as a a coordinator for managing clusters, including storage and brokers.
impl DanubeService {
    pub(crate) fn new(service_config: ServiceConfiguration) -> Self {
        DanubeService {
            config: service_config,
            resources: DanubeResources::new(),
            broker: Arc::new(BrokerService::new()),
        }
    }

    pub(crate) async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let socket_addr = self.config.broker_addr.clone();

        let metadata_store: MetadataStorage = if let Some(etcd_addr) = self.config.etcd_addr.clone()
        {
            MetadataStorage::EtcdStore(EtcdMetadataStore::new(etcd_addr).await?)
        } else {
            MetadataStorage::MemoryStore(MemoryMetadataStore::new().await?)
        };

        let storage = storage::memory_segment_storage::SegmentStore::new();

        let broker_server =
            danube_server::DanubeServerImpl::new(self.broker.clone(), self.config.broker_addr);

        broker_server.start();

        //TODO! here we may want to start the MetadataEventSynchronizer & CoordinationService

        Ok(())
    }
}
