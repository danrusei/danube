mod broker_server;
mod broker_service;
mod consumer;
mod controller;
mod danube_service;
mod dispatcher;
mod metadata_store;
mod namespace;
mod policies;
mod producer;
mod resources;
mod service_configuration;
mod storage;
mod subscription;
mod topic;
mod utils;

use crate::danube_service::DanubeService;
use crate::service_configuration::ServiceConfiguration;

use clap::Parser;
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let broker_addr: std::net::SocketAddr = args.broker_addr.parse()?;

    let broker_config = ServiceConfiguration {
        cluster_name: args.cluster_name,
        broker_addr: broker_addr,
        meta_store_addr: args.meta_store_addr,
    };

    let mut danube = DanubeService::new(broker_config);

    info!("Start the Danube Broker Service");
    danube.start().await.expect("the broker unable to start");

    Ok(())
}
