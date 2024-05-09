mod danube_service;
mod service_configuration;

use crate::danube_service::DanubeService;
use crate::service_configuration::ServiceConfiguration;

use clap::Parser;
use tonic::transport::Server;
use tonic::{Request, Response};

pub(crate) mod proto {
    include!("../../proto/danube.rs");
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config_file: Option<String>,

    /// Danube Broker advertised address
    #[arg(short, long, default_value = "[::1]:6650")]
    advertised_address: String,

    /// ETCD address
    #[arg(short, long)]
    etcd_addr: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let broker_addr: std::net::SocketAddr = args.advertised_address.parse()?;

    let broker_config = ServiceConfiguration {
        broker_addr: broker_addr,
    };

    let danube = DanubeService::new(broker_config);

    danube.start().await;

    Ok(())
}
