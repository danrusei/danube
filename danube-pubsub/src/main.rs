mod consume;
mod produce;

use anyhow::Result;
use clap::{Parser, Subcommand};
use consume::Consume;
use produce::Produce;

#[derive(Debug, Parser)]
#[command(name = "danube-pubsub")]
#[command(about = "CLI for managing the Danube pub/sub platform")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Produce(Produce),
    Consume(Consume),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Produce(produce) => produce::handle_produce(produce).await?,
        Commands::Consume(consume) => consume::handle_consume(consume).await?,
    }

    Ok(())
}
