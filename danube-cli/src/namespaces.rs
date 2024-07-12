use crate::proto::{namespace_admin_client::NamespaceAdminClient, NamespaceRequest};
use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub(crate) struct Namespaces {
    #[command(subcommand)]
    command: NamespacesCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum NamespacesCommands {
    Topics { namespace: String },
    Policies { namespace: String },
    Create { namespace: String },
    Delete { namespace: String },
}

#[allow(unreachable_code)]
pub async fn handle_command(namespaces: Namespaces) -> Result<(), Box<dyn std::error::Error>> {
    let client = NamespaceAdminClient::connect("http://[::1]:50051").await?;

    match namespaces.command {
        // Get the list of topics of a namespace
        NamespacesCommands::Topics { namespace } => {
            let _namespace = namespace;
            // to implement
            todo!();
            let request = NamespaceRequest { name: namespace };
            let response = client.get_namespace_topics(request).await?;
            println!("Topics: {:?}", response.into_inner().topics);
        }
        // Get the configuration policies of a namespace
        NamespacesCommands::Policies { namespace } => {
            let _namespace = namespace;
            // to implement
            todo!();
            let request = NamespaceRequest { name: namespace };
            let response = client.get_namespace_policies(request).await?;
            println!("Policies: {:?}", response.into_inner().policies);
        }
        // Create a new namespace
        NamespacesCommands::Create { namespace } => {
            let _namespace = namespace;
            // to implement
            todo!();
            let request = NamespaceRequest { name: namespace };
            let response = client.create_namespace(request).await?;
            println!("Namespace Created: {:?}", response.into_inner().success);
        }
        // Deletes a namespace. The namespace needs to be empty
        NamespacesCommands::Delete { namespace } => {
            let _namespace = namespace;
            // to implement
            todo!();
            let request = NamespaceRequest { name: namespace };
            let response = client.delete_namespace(request).await?;
            println!("Namespace Deleted: {:?}", response.into_inner().success);
        }
    }

    Ok(())
}
