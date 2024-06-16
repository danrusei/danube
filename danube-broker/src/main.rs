mod broker_server;
mod broker_service;
mod consumer;
mod danube_service;
mod dispatcher;
mod metadata_store;
mod namespace;
mod policies;
mod producer;
mod resources;
mod schema;
mod service_configuration;
mod storage;
mod subscription;
mod topic;
mod utils;

use std::sync::Arc;

use crate::{
    broker_service::BrokerService,
    danube_service::{DanubeService, LeaderElection, LoadManager, LocalCache, Syncronizer},
    metadata_store::{
        EtcdMetadataStore, MemoryMetadataStore, MetadataStorage, MetadataStoreConfig,
    },
    resources::{Resources, LEADER_SELECTION_PATH},
    service_configuration::ServiceConfiguration,
};

use anyhow::Result;
use clap::Parser;
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber;

pub(crate) mod proto {
    include!("../../proto/danube.rs");
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Cluster Name
    #[arg(short, long)]
    cluster_name: String,

    /// Path to config file
    #[arg(short, long)]
    config_file: Option<String>,

    /// Danube Broker advertised address
    #[arg(short, long, default_value = "[::1]:6650")]
    broker_addr: String,

    /// Metadata store address
    #[arg(short, long)]
    meta_store_addr: Option<String>,

    /// List of namespaces (comma-separated)
    #[arg(short = 'n', long, value_parser = parse_namespaces_list)]
    namespaces: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let broker_addr: std::net::SocketAddr = args.broker_addr.parse()?;

    // configuration settings for a Danube broker service
    // includes various parameters that control the behavior and performance of the broker
    // TODO! read from a config file, like danube.conf
    let service_config = ServiceConfiguration {
        cluster_name: args.cluster_name,
        broker_addr: broker_addr,
        meta_store_addr: args.meta_store_addr,
        bootstrap_namespaces: args.namespaces,
    };

    // initialize the storage layer for Danube Metadata
    let store_config = MetadataStoreConfig::new();
    let metadata_store: MetadataStorage =
        if let Some(etcd_addr) = service_config.meta_store_addr.clone() {
            MetadataStorage::EtcdStore(EtcdMetadataStore::new(etcd_addr, store_config).await?)
        } else {
            MetadataStorage::MemoryStore(MemoryMetadataStore::new(store_config).await?)
        };

    // caching metadata locally to reduce the number of remote calls to Metadata Store
    let local_cache = LocalCache::new();

    // convenient functions to handle the metadata and configurations required
    // for managing the cluster, namespaces & topics
    let mut resources = Resources::new(local_cache.clone(), metadata_store.clone());

    // The synchronizer ensures that metadata & configuration settings across different brokers remains consistent.
    // using the client Producers to distribute metadata updates across brokers.
    let syncroniser = Syncronizer::new();

    // the broker service, is responsible to reliable deliver the messages from producers to consumers.
    let broker_service = BrokerService::new(resources.clone());
    let broker_id = broker_service.broker_id;

    // the service selects one broker per cluster to be the leader to coordinate and take assignment decision.
    let mut leader_election_service = LeaderElection::new(
        metadata_store.clone(),
        LEADER_SELECTION_PATH,
        broker_service.broker_id,
    );

    // Load Manager, monitor and distribute load across brokers.
    let load_manager = LoadManager::new(broker_service.broker_id);

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

fn parse_namespaces_list(s: &str) -> Result<Vec<String>> {
    Ok(s.split(',')
        .map(|nam| nam.trim().to_string())
        .collect::<Vec<String>>())
}
