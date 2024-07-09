use crate::proto::{broker_admin_client::BrokerAdminClient, Empty};
use clap::{Args, Subcommand};

use prettytable::{format, Cell, Row, Table};

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
    let mut client = BrokerAdminClient::connect("http://[::1]:50051").await?;

    match brokers.command {
        BrokersCommands::List => {
            let response = client.list_brokers(Empty {}).await?;
            let brokers = response.into_inner().brokers;

            // Create a table and add headers
            let mut table = Table::new();
            table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
            table.add_row(Row::new(vec![
                Cell::new("BROKER ID"),
                Cell::new("BROKER ADDRESS"),
                Cell::new("BROKER ROLE"),
            ]));

            // Add each broker's information as a row in the table
            for broker in brokers {
                table.add_row(Row::new(vec![
                    Cell::new(&broker.broker_id),
                    Cell::new(&broker.broker_addr),
                    Cell::new(&broker.broker_role),
                ]));
            }

            // Print the table
            table.printstd();
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
