mod broker_register;
mod leader_election;
mod load_manager;
mod local_cache;
mod syncronizer;

pub(crate) use broker_register::register_broker;
pub(crate) use leader_election::{LeaderElection, LeaderElectionState};
pub(crate) use load_manager::load_report::{generate_load_report, LoadReport};
pub(crate) use load_manager::LoadManager;
pub(crate) use local_cache::LocalCache;
pub(crate) use syncronizer::Syncronizer;

use anyhow::Result;
use danube_client::DanubeClient;
use etcd_client::Client;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{self, sleep, Duration};
use tracing::{error, info, trace, warn};

use crate::{
    broker_server,
    broker_service::BrokerService,
    metadata_store::{etcd_watch_prefixes, MetaOptions, MetadataStorage, MetadataStore},
    namespace::{DEFAULT_NAMESPACE, SYSTEM_NAMESPACE},
    policies::Policies,
    resources::{Resources, BASE_BROKER_LOAD_PATH, BASE_BROKER_PATH},
    service_configuration::ServiceConfiguration,
    topic::SYSTEM_TOPIC,
    utils::join_path,
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
            "Setting up the cluster {}",
            self.service_config.cluster_name
        );

        // Cluster metadata setup
        //==========================================================================
        let _ = self
            .resources
            .cluster
            .create_cluster(&self.service_config.cluster_name)
            .await;

        // register the local broker to cluster
        let ttl = 40; // Time to live for the lease in seconds
        register_broker(
            self.meta_store.clone(),
            &self.broker_id.to_string(),
            &self.service_config.broker_addr.to_string(),
            ttl,
        )
        .await?;

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

        info!("cluster metadata setup completed");

        // Not used yet, will be used for persistent topic storage, which is not yet implemented
        // let _storage = storage::memory_segment_storage::SegmentStore::new();

        // Start the Broker GRPC server
        //==========================================================================

        let grpc_server = broker_server::DanubeServerImpl::new(
            self.broker.clone(),
            self.service_config.broker_addr,
        );

        // Create a oneshot channel for readiness signaling
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        let server_handle = grpc_server.start(ready_tx).await;

        info!(" Started the Broker GRPC server");

        // Start the Syncronizer
        //==========================================================================

        // Wait for the server to signal that it has started
        ready_rx.await?;

        // it is needed by syncronizer in order to publish messages on meta_topic
        let client = DanubeClient::builder()
            .service_url("http://[::1]:6650")
            .build()?;

        let _ = self.syncronizer.with_client(client);

        // TODO! create producer / consumer and use it

        // Start the Leader Election Service
        //==========================================================================

        // to be configurable
        let leader_check_interval = time::interval(Duration::from_secs(10));

        let mut leader_election_cloned = self.leader_election.clone();

        tokio::spawn(async move {
            leader_election_cloned.start(leader_check_interval).await;
        });
        info!("Started the Leader Election service");

        // Start the Local Cache
        //==========================================================================

        // Fetch initial data, populate cache & watch for Events to update local cache
        let client = self.meta_store.get_client();
        if let Some(client) = client.clone() {
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
        let leader_election_cloned = self.leader_election.clone();
        // Process the ETCD Watch events
        tokio::spawn(async move {
            load_manager_cloned
                .start(rx_event, broker_id_cloned, leader_election_cloned)
                .await
        });

        let broker_service_cloned = Arc::clone(&self.broker);
        let meta_store_cloned = self.meta_store.clone();

        info!("Started the Load Manager service.");

        // Publish periodic Load Reports
        // This enable the broker to register with Load Manager
        tokio::spawn(async move {
            post_broker_load_report(broker_service_cloned, meta_store_cloned).await
        });

        // Watch for events of Broker's interest
        let broker_service_cloned = Arc::clone(&self.broker);
        if let Some(client) = client {
            self.watch_events_for_broker(client, broker_service_cloned)
                .await;
        }

        // Wait for the server task to complete
        server_handle.await?;

        Ok(())
    }

    async fn watch_events_for_broker(
        &self,
        client: Client,
        broker_service: Arc<Mutex<BrokerService>>,
    ) {
        let (tx_event, mut rx_event) = tokio::sync::mpsc::channel(32);
        let broker_id = self.broker_id;

        // watch for ETCD events
        tokio::spawn(async move {
            let mut prefixes = Vec::new();
            let topic_assignment_path = join_path(&[BASE_BROKER_PATH, &broker_id.to_string()]);
            prefixes.extend([topic_assignment_path].into_iter());
            let _ = etcd_watch_prefixes(client, prefixes, tx_event).await;
        });

        //process the events
        tokio::spawn(async move {
            while let Some(event) = rx_event.recv().await {
                info!(
                    "{}",
                    format!("A new Watch event has been received {:?}", event)
                );

                let parts: Vec<_> = event.key.split('/').collect();
                if parts.len() < 3 {
                    warn!("Invalid key format {}", event.key);
                    return;
                }

                let prefix = format!("/{}/{}", parts[1], parts[2]);

                match prefix.as_str() {
                    path if path == BASE_BROKER_PATH => {
                        // the key format should be (BASE_BROKER_PATH, broker_id, topic_name)
                        // like: /cluster/brokers/1685432450824113596/default/my_topic
                        if parts.len() != 6 {
                            warn!("Invalid key format: {}", event.key);
                            return;
                        }

                        let topic_name = format!("/{}/{}", parts[4], parts[5]);
                        match event.event_type {
                            etcd_client::EventType::Put => {
                                // wait a sec so the LocalCache receive the updates from the persistent metadata
                                sleep(Duration::from_secs(2)).await;
                                let mut broker_service = broker_service.lock().await;
                                match broker_service.create_topic(&topic_name).await {
                                    Ok(()) => info!(
                                        "The topic {} , was successfully created on broker {}",
                                        topic_name, broker_id
                                    ),
                                    Err(err) => {
                                        error!("Unable to create the topic due to error: {}", err)
                                    }
                                }
                            }
                            etcd_client::EventType::Delete => {
                                let mut broker_service = broker_service.lock().await;
                                match broker_service.delete_topic(&topic_name).await {
                                    Ok(_) => info!(
                                        "The topic {} , was successfully deleted from the broker {}",
                                        topic_name, broker_id
                                    ),
                                    Err(err) => {
                                        error!("Unable to delete the topic due to error: {}", err)
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        warn!("Shouldn't reach this arm, the prefix is {}", prefix)
                    }
                }
            }
        });
    }

    // Checks whether the broker owns a specific topic
    #[allow(dead_code)]
    pub(crate) async fn check_topic_ownership(&self, topic_name: &str) -> bool {
        self.load_manager
            .check_ownership(self.broker_id, topic_name)
            .await
    }
}

async fn create_namespace_if_absent(
    resources: &mut Resources,
    _namespace_name: &str,
) -> Result<()> {
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
        let load_report: LoadReport = generate_load_report(topics_len, topics);
        if let Ok(value) = serde_json::to_value(&load_report) {
            let path = join_path(&[BASE_BROKER_LOAD_PATH, &broker_id.to_string()]);
            match meta_store.put(&path, value, MetaOptions::None).await {
                Ok(_) => trace!(
                    "Broker {} posted a new Load Report: {:?}",
                    broker_id,
                    &load_report
                ),
                Err(err) => trace!(
                    "Broker {} unable to post Load Report dues to this issue {}",
                    broker_id,
                    err
                ),
            }
        }
    }
}

#[allow(dead_code)]
pub(crate) enum LookupResult {
    BrokerUrl(String),
    RedirectUrl(String),
}
