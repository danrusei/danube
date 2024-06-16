mod leader_election;
mod load_manager;
mod local_cache;
mod syncronizer;

pub(crate) use leader_election::{LeaderElection, LeaderElectionState};
pub(crate) use load_manager::load_report::{generate_load_report, LoadReport};
pub(crate) use load_manager::LoadManager;
pub(crate) use local_cache::LocalCache;
pub(crate) use syncronizer::Syncronizer;

use anyhow::Result;
use danube_client::DanubeClient;
use etcd_client::PutOptions;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, Duration};
use tracing::info;

use crate::{
    broker_server,
    broker_service::{self, BrokerService},
    metadata_store::{
        EtcdMetadataStore, MemoryMetadataStore, MetaOptions, MetadataStorage, MetadataStore,
        MetadataStoreConfig,
    },
    namespace::{DEFAULT_NAMESPACE, SYSTEM_NAMESPACE},
    policies::Policies,
    resources::{self, join_path, Resources, BASE_BROKER_LOAD_PATH},
    service_configuration::ServiceConfiguration,
    storage,
    topic::SYSTEM_TOPIC,
};

// Danube Service has cluster and local Broker management & coordination responsabilities
//
// Namespace Creation and Management:
// Allow users to create & delete namespaces within the cluster.
// This includes specifying policies and configurations specific to each namespace.
//
// LookUp Service:
// Provide a mechanism for clients to discover the brokers that own the desired topics.
// This is essential for producers to know where to send messages and for consumers to know where to fetch messages from.
// Handle client redirections if the broker ownership of a topic or partition changes.
//
// Leader Election:
// Leader Election service is needed for critical tasks such as topic assignment to brokers and partitioning.
// Load Manager is using this service, as only one broker is selected to make the load usage calculations and post the results.
// Should be selected one broker leader per cluster, who takes the decissions.
//
// Load Manager:
// The Load Manager monitors and distributes load across brokers by managing topic and partition assignments.
// It implements rebalancing logic to redistribute topics/partitions when brokers join or leave the cluster
// and is responsible for failover mechanisms to handle broker failures.
//
// Syncronizer
// The synchronizer ensures that metadata and configuration settings across different brokers remain consistent.
// It propagates changes to metadata and configuration settings using client Producers and Consumers.
// This is in addition to Metadata Storage watch events, allowing brokers to process metadata updates
// even if there was a communication glitch or the broker was unavailable for a short period, potentially missing the Store Watch events.
//
// Monitoring and Metrics:
// Collect and provide metrics related to namespace usage, such as message rates, storage usage, and throughput.
// Integrate with monitoring and alerting systems to provide insights into namespace performance and detect anomalies.
//
// Resource Quotas:
// Implement and enforce resource quotas to ensure fair usage of resources among different namespaces.
// This includes limiting the number of topics, message rates, and storage usage.
#[derive(Debug)]
pub(crate) struct DanubeService {
    broker_id: u64,
    broker: Arc<Mutex<BrokerService>>,
    service_config: ServiceConfiguration,
    meta_store: MetadataStorage,
    local_cache: LocalCache,
    resources: Resources,
    leader_election: LeaderElection,
    syncronizer: Syncronizer,
    load_manager: LoadManager,
}

// DanubeService act as a a coordinator for managing clusters, including storage and brokers.
impl DanubeService {
    pub(crate) fn new(
        broker_id: u64,
        broker: Arc<Mutex<BrokerService>>,
        service_config: ServiceConfiguration,
        meta_store: MetadataStorage,
        local_cache: LocalCache,
        resources: Resources,
        leader_election: LeaderElection,
        syncronizer: Syncronizer,
        load_manager: LoadManager,
    ) -> Self {
        DanubeService {
            broker_id,
            broker,
            service_config,
            meta_store,
            local_cache,
            resources,
            leader_election,
            syncronizer,
            load_manager,
        }
    }

    pub(crate) async fn start(&mut self) -> Result<()> {
        info!(
            "Setting up the cluster {} with metadata-store {}",
            self.service_config.cluster_name, "etcd"
        );

        // Cluster metadata setup
        //==========================================================================
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

        //create bootstrap namespaces
        for namespace in &self.service_config.bootstrap_namespaces {
            create_namespace_if_absent(&mut self.resources, &namespace).await?;
        }

        //cluster metadata setup completed

        // Not used yet, will be used for persistent topic storage, which is not yet implemented
        // let _storage = storage::memory_segment_storage::SegmentStore::new();

        // Start the Broker GRPC server
        //==========================================================================

        let grpc_server = broker_server::DanubeServerImpl::new(
            self.broker.clone(),
            self.service_config.broker_addr,
        );

        grpc_server.start().await?;

        info!(" Started the Broker GRPC server");

        // Start the Syncronizer
        //==========================================================================

        // it is needed by syncronizer in order to publish messages on meta_topic
        let client = DanubeClient::builder()
            .service_url("http://[::1]:6650")
            .build()?;

        self.syncronizer.with_client(client);

        // TODO! create producer / consumer and use it

        // Start the Leader Election Service
        //==========================================================================

        self.leader_election.start();

        info!("Started the Leader Election service");

        // Start the Local Cache
        //==========================================================================

        // Fetch initial data, populate cache & watch for Events to update local cache
        let mut client = self.meta_store.get_client();
        if let Some(client) = client {
            let local_cache_cloned = self.local_cache.clone();
            let rx_event = self.local_cache.populate_start_local_cache(client).await?;
            // Process the ETCD Watch events
            tokio::spawn(async move { local_cache_cloned.process_event(rx_event).await });
        }

        info!("Started the Local Cache service.");

        // Start the Load Manager Service
        //==========================================================================
        // at this point the broker will become visible to the rest of the brokers
        // by creating the registration and also

        let rx_event = self.load_manager.bootstrap(self.broker_id).await?;

        let mut load_manager_cloned = self.load_manager.clone();

        let broker_id_cloned = self.broker_id;
        // Process the ETCD Watch events
        tokio::spawn(async move { load_manager_cloned.start(rx_event, broker_id_cloned).await });

        let broker_service_cloned = Arc::clone(&self.broker);
        let meta_store_cloned = self.meta_store.clone();

        info!("Started the Load Manager service.");

        // Publish periodic Load Reports
        // This enable the broker to register with Load Manager
        tokio::spawn(
            async move { post_broker_load_report(broker_service_cloned, meta_store_cloned) },
        );

        Ok(())
    }

    // Checks whether the broker owns a specific topic
    pub(crate) async fn check_topic_ownership(&self, topic_name: &str) -> bool {
        self.load_manager
            .check_ownership(self.broker_id, topic_name)
            .await
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
        let _policies = resources.namespace.get_policies(DEFAULT_NAMESPACE)?;
    }
    Ok(())
}

async fn post_broker_load_report(
    broker_service: Arc<Mutex<BrokerService>>,
    mut meta_store: MetadataStorage,
) {
    let mut topics: Vec<String>;
    let mut broker_id;
    let mut interval = time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        {
            let broker_service = broker_service.lock().await;
            topics = broker_service.get_topics().into_iter().cloned().collect();
            broker_id = broker_service.broker_id;
        }
        let topics_len = topics.len();
        let load_repot: LoadReport = generate_load_report(topics_len, topics);
        if let Ok(value) = serde_json::to_value(load_repot) {
            let path = join_path(&[BASE_BROKER_LOAD_PATH, &broker_id.to_string()]);
            meta_store.put(&path, value, MetaOptions::None);
        }
    }
}

pub(crate) enum LookupResult {
    BrokerUrl(String),
    RedirectUrl(String),
}
