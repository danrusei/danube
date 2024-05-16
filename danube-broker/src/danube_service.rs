use crate::metadata_store::{EtcdMetadataStore, MemoryMetadataStore, MetadataStorage};

use crate::resources::DanubeResources;
use crate::service_configuration::ServiceConfiguration;
use crate::{broker_service, storage};

#[derive(Debug)]
pub(crate) struct DanubeService {
    config: ServiceConfiguration,
    resources: DanubeResources,
}

impl DanubeService {
    pub(crate) fn new(service_config: ServiceConfiguration) -> Self {
        DanubeService {
            config: service_config,
            resources: DanubeResources::new(),
        }
    }

    pub(crate) async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let socket_addr = self.config.broker_addr.clone();

        let metadata_store: MetadataStorage = if let Some(etcd_addr) = self.config.etcd_addr.clone()
        {
            MetadataStorage::EtcdStore(EtcdMetadataStore::new(etcd_addr).await?)
        } else {
            MetadataStorage::MemoryStore(MemoryMetadataStore::new().await?)
        };

        let storage = storage::memory_segment_storage::SegmentStore::new();

        // initialize the broker
        let broker = broker_service::BrokerService::new(self.config.broker_addr);

        //start the broker
        broker.start().await?;

        //TODO! here we may want to start the MetadataEventSynchronizer & CoordinationService

        Ok(())
    }
}
