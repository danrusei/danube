mod admin;
mod broker_metrics;
mod broker_server;
mod broker_service;
mod consumer;
mod danube_service;
mod dispatcher;
mod error_message;
mod metadata_store;
mod policies;
mod producer;
mod resources;
mod schema;
mod service_configuration;
mod storage;
mod subscription;
mod topic;
mod utils;

use std::{fs::read_to_string, path::Path, sync::Arc};

use crate::{
    broker_metrics::init_metrics,
    broker_service::BrokerService,
    danube_service::{DanubeService, LeaderElection, LoadManager, LocalCache, Syncronizer},
    metadata_store::{EtcdMetadataStore, MetadataStorage, MetadataStoreConfig},
    resources::{Resources, LEADER_ELECTION_PATH},
    service_configuration::ServiceConfiguration,
};

use anyhow::{anyhow, Result};
use clap::Parser;
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber;

pub(crate) mod proto {
    include!("proto/danube.rs");
}

pub(crate) mod admin_proto {
    include!("proto/danube_admin.rs");
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to config file (required)
    #[arg(short = 'c', long)]
    config_file: String,

    /// Danube Broker advertised address (optional, overrides config file)
    #[arg(short = 'b', long)]
    broker_addr: Option<String>,

    /// Danube Broker Admin address (optional, overrides config file)
    #[arg(short = 'a', long)]
    admin_addr: Option<String>,

    /// Prometheus Exporter http address (optional, overrides config file)
    #[arg(short = 'p', long)]
    prom_exporter: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();

    // Load the configuration from the specified YAML file
    let config_content = read_to_string(Path::new(&args.config_file))?;
    let mut service_config: ServiceConfiguration = serde_yaml::from_str(&config_content)?;

    // If `broker_addr` is provided via command-line args, override the value from the config file
    if let Some(broker_addr) = args.broker_addr {
        service_config.broker_addr = broker_addr;
    }

    // If `admin_addr` is provided via command-line args, override the value from the config file
    if let Some(admin_addr) = args.admin_addr {
        service_config.admin_addr = admin_addr;
    }

    // If `prom_exporter` is provided via command-line args, override the value from the config file
    if let Some(prom_exporter) = args.prom_exporter {
        service_config.prom_exporter = Some(prom_exporter);
    }

    // Init metrics with or without prometheus exporter
    if let Some(prometheus_exporter) = service_config.prom_exporter.clone() {
        let prom_addr: std::net::SocketAddr = prometheus_exporter.parse()?;
        init_metrics(Some(prom_addr));
    } else {
        init_metrics(None)
    }

    // initialize the storage layer for Danube Metadata
    let store_config = MetadataStoreConfig::new();
    let metadata_store: MetadataStorage =
        if let Some(etcd_addr) = service_config.meta_store_addr.clone() {
            info!("Use ETCD storage as metadata persistent store");
            MetadataStorage::EtcdStore(EtcdMetadataStore::new(etcd_addr, store_config).await?)
        } else {
            return Err(anyhow!("ETCD meta store address is required"));
            // MemoryStore is not yet supported
            // MetadataStorage::MemoryStore(MemoryMetadataStore::new(store_config).await?)
        };

    // caching metadata locally to reduce the number of remote calls to Metadata Store
    let local_cache = LocalCache::new();

    // convenient functions to handle the metadata and configurations required
    // for managing the cluster, namespaces & topics
    let resources = Resources::new(local_cache.clone(), metadata_store.clone());

    // The synchronizer ensures that metadata & configuration settings across different brokers remains consistent.
    // using the client Producers to distribute metadata updates across brokers.
    let syncroniser = Syncronizer::new();

    // the broker service, is responsible to reliable deliver the messages from producers to consumers.
    let broker_service = BrokerService::new(resources.clone());
    let broker_id = broker_service.broker_id;

    // the service selects one broker per cluster to be the leader to coordinate and take assignment decision.
    let leader_election_service = LeaderElection::new(
        metadata_store.clone(),
        LEADER_ELECTION_PATH,
        broker_service.broker_id,
    );

    // Load Manager, monitor and distribute load across brokers.
    let load_manager = LoadManager::new(broker_service.broker_id, metadata_store.clone());

    let broker: Arc<Mutex<BrokerService>> = Arc::new(Mutex::new(broker_service));

    // DanubeService coordinate and start all the services
    let mut danube = DanubeService::new(
        broker_id,
        Arc::clone(&broker),
        service_config,
        metadata_store,
        local_cache,
        resources,
        leader_election_service,
        syncroniser,
        load_manager,
    );

    info!("Start the Danube Service");
    danube.start().await.expect("the broker unable to start");

    info!("The Danube Service has started succesfully");

    Ok(())
}
