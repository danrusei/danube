use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::{
    broker_server,
    broker_service::{self, BrokerService},
    controller::{self, Controller, LeaderElection, LocalCache},
    metadata_store::{
        EtcdMetadataStore, MemoryMetadataStore, MetadataStorage, MetadataStoreConfig,
    },
    namespace::{DEFAULT_NAMESPACE, SYSTEM_NAMESPACE},
    policies::Policies,
    resources::{self, Resources},
    service_configuration::ServiceConfiguration,
    storage,
    topic::SYSTEM_TOPIC,
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

        let local_cache = LocalCache::new();

        let mut resources = Resources::new(local_cache.clone(), metadata_store.clone());

        // instantiate the controller
        {
            let mut broker = self.broker.lock().await;
            let broker_id = broker.broker_id;
            let controller = Controller::new(
                broker_id,
                Arc::clone(&self.broker),
                local_cache,
                metadata_store,
            );
        }
        info!(
            "Setting up the cluster {} with metadata-store {}",
            self.config.cluster_name, "etcd"
        );
        resources.cluster.create_cluster(
            &self.config.cluster_name,
            self.config.broker_addr.to_string(),
        );

        //create the default Namespace
        create_namespace_if_absent(&mut resources, DEFAULT_NAMESPACE).await?;

        //create system Namespace
        create_namespace_if_absent(&mut resources, SYSTEM_NAMESPACE).await?;

        //create system topic
        if !resources.topic.topic_exists(SYSTEM_TOPIC).await? {
            resources.topic.create_topic(SYSTEM_TOPIC, 0).await?;
        }

        //cluster metadata setup completed

        //create bootstrap namespaces
        for namespace in &self.config.bootstrap_namespaces {
            create_namespace_if_absent(&mut resources, &namespace).await?;
        }

        // Not used yet, will be used for persistent topic storage, which is not yet implemented
        let _storage = storage::memory_segment_storage::SegmentStore::new();

        let grpc_server =
            broker_server::DanubeServerImpl::new(self.broker.clone(), self.config.broker_addr);

        grpc_server.start().await?;

        //TODO! here we may want to start the MetadataEventSynchronizer and maybe the LeaderElection

        Ok(())
    }
}

async fn create_namespace_if_absent(resources: &mut Resources, namespace_name: &str) -> Result<()> {
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
    Ok(())
}
