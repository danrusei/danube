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
    //broker_id: Option<u64>,
    broker: Arc<Mutex<BrokerService>>,
    controller: Controller,
    service_config: ServiceConfiguration,
    resources: Resources,
}

// DanubeService act as a a coordinator for managing clusters, including storage and brokers.
impl DanubeService {
    pub(crate) fn new(
        broker: Arc<Mutex<BrokerService>>,
        controller: Controller,
        service_config: ServiceConfiguration,
        resources: Resources,
    ) -> Self {
        DanubeService {
            broker,
            controller,
            service_config,
            resources,
        }
    }

    pub(crate) async fn start(&mut self) -> Result<()> {
        info!(
            "Setting up the cluster {} with metadata-store {}",
            self.service_config.cluster_name, "etcd"
        );
        self.resources.cluster.create_cluster(
            &self.service_config.cluster_name,
            self.service_config.broker_addr.to_string(),
        );

        //create the default Namespace
        create_namespace_if_absent(&mut self.resources, DEFAULT_NAMESPACE).await?;

        //create system Namespace
        create_namespace_if_absent(&mut self.resources, SYSTEM_NAMESPACE).await?;

        //create system topic
        if !self.resources.topic.topic_exists(SYSTEM_TOPIC).await? {
            self.resources.topic.create_topic(SYSTEM_TOPIC, 0).await?;
        }

        //cluster metadata setup completed

        //create bootstrap namespaces
        for namespace in &self.service_config.bootstrap_namespaces {
            create_namespace_if_absent(&mut self.resources, &namespace).await?;
        }

        // Not used yet, will be used for persistent topic storage, which is not yet implemented
        // let _storage = storage::memory_segment_storage::SegmentStore::new();

        let grpc_server = broker_server::DanubeServerImpl::new(
            self.broker.clone(),
            self.service_config.broker_addr,
        );

        grpc_server.start().await?;

        // The Controller is responsible of starting the Syncronizer, LeaderElection and Load Manager Services
        self.controller.start();

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
