mod brokers;
mod namespaces;
mod shared;
mod topics;

use brokers::Brokers;
use namespaces::Namespaces;
use topics::Topics;

use clap::{Parser, Subcommand};

pub mod proto {
    include!("proto/danube_admin.rs");
}

#[derive(Debug, Parser)]
#[command(name = "danube-cli")]
#[command(about = "CLI for managing the Danube pub/sub platform", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Brokers(Brokers),
    Namespaces(Namespaces),
    Topics(Topics),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Brokers(brokers) => brokers::handle_command(brokers).await?,
        Commands::Namespaces(namespaces) => namespaces::handle_command(namespaces).await?,
        Commands::Topics(topics) => topics::handle_command(topics).await?,
    }

    Ok(())
}
