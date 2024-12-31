mod consume;
mod produce;

use anyhow::Result;
use clap::{Parser, Subcommand};
use consume::Consume;
use produce::Produce;

#[derive(Debug, Parser)]
#[command(name = "danube-pubsub")]
#[command(about = "A command-line tool to interact with Danube service")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "produce messages for the topic")]
    Produce(Produce),
    #[command(about = "consume messages from the topic")]
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
