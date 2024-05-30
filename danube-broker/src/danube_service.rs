use std::sync::Arc;
use tokio::sync::Mutex;

use crate::broker_server;
use crate::metadata_store::{EtcdMetadataStore, MemoryMetadataStore, MetadataStorage};

use crate::resources::Resources;
use crate::service_configuration::ServiceConfiguration;
use crate::{broker_service::BrokerService, storage};

#[derive(Debug)]
pub(crate) struct DanubeService {
    config: ServiceConfiguration,
    resources: Resources,
    broker: Arc<Mutex<BrokerService>>,
}

// DanubeService act as a a coordinator for managing clusters, including storage and brokers.
impl DanubeService {
    pub(crate) fn new(service_config: ServiceConfiguration) -> Self {
        DanubeService {
            config: service_config,
            resources: Resources::new(),
            broker: Arc::new(Mutex::new(BrokerService::new())),
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

        let grpc_server =
            broker_server::DanubeServerImpl::new(self.broker.clone(), self.config.broker_addr);

        grpc_server.start().await?;

        //TODO! here we may want to start the MetadataEventSynchronizer & CoordinationService

        Ok(())
    }
}
