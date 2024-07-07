use crate::proto::{broker_admin_client::BrokerAdminClient, Empty};
use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub(crate) struct Brokers {
    #[command(subcommand)]
    command: BrokersCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum BrokersCommands {
    List,
    LeaderBroker,
    Namespaces,
}

#[allow(unreachable_code)]
pub async fn handle_command(brokers: Brokers) -> Result<(), Box<dyn std::error::Error>> {
    let client = BrokerAdminClient::connect("http://[::1]:50051").await?;

    match brokers.command {
        BrokersCommands::List => {
            todo!();
            let response = client.list_brokers(Empty {}).await?;
            println!("Active Brokers: {:?}", response.into_inner().brokers);
        }
        BrokersCommands::LeaderBroker => {
            todo!();
            let response = client.get_leader_broker(Empty {}).await?;
            println!("Leader Broker: {:?}", response.into_inner().leader);
        }
        BrokersCommands::Namespaces => {
            todo!();
            let response = client.list_namespaces(Empty {}).await?;
            println!("Namespaces: {:?}", response.into_inner().namespaces);
        }
    }

    Ok(())
}
