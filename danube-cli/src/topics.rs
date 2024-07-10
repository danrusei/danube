use crate::proto::{
    topic_admin_client::TopicAdminClient, NamespaceRequest, PartitionedTopicRequest,
    SubscriptionRequest, TopicRequest,
};
use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub(crate) struct Topics {
    #[command(subcommand)]
    command: TopicsCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum TopicsCommands {
    List {
        namespace: String,
    },
    Create {
        topic: String,
    },
    CreatePartitionedTopic {
        topic: String,
        #[arg(short, long, default_value_t = 0)]
        partitions: i32,
    },
    Delete {
        topic: String,
    },
    Unsubscribe {
        topic: String,
        #[arg(short, long)]
        subscription: String,
    },
    Subscriptions {
        topic: String,
    },
    CreateSubscription {
        #[arg(short, long)]
        subscription: String,
        topic: String,
    },
}

#[allow(unreachable_code)]
pub async fn handle_command(topics: Topics) -> Result<(), Box<dyn std::error::Error>> {
    let client = TopicAdminClient::connect("http://[::1]:50051").await?;

    match topics.command {
        TopicsCommands::List { namespace } => {
            let _namespace = namespace;
            // to implement
            todo!();
            let request = NamespaceRequest { name: namespace };
            let response = client.list_topics(request).await?;
            println!("Topics: {:?}", response.into_inner().topics);
        }
        TopicsCommands::Create { topic } => {
            let _topic = topic;
            // to implement
            todo!();
            let request = TopicRequest { name: topic };
            let response = client.create_topic(request).await?;
            println!("Topic Created: {:?}", response.into_inner().success);
        }
        TopicsCommands::CreatePartitionedTopic { topic, partitions } => {
            let _topic = topic;
            let _partitions = partitions;
            // to implement
            todo!();
            let request = PartitionedTopicRequest {
                name: topic,
                partitions,
            };
            let response = client.create_partitioned_topic(request).await?;
            println!(
                "Partitioned Topic Created: {:?}",
                response.into_inner().success
            );
        }
        TopicsCommands::Delete { topic } => {
            let _topic = topic;
            // to implement
            todo!();
            let request = TopicRequest { name: topic };
            let response = client.delete_topic(request).await?;
            println!("Topic Deleted: {:?}", response.into_inner().success);
        }
        TopicsCommands::Unsubscribe {
            topic,
            subscription,
        } => {
            let _topic = topic;
            let _subscription = subscription;
            // to implement
            todo!();
            let request = SubscriptionRequest {
                topic,
                subscription,
            };
            let response = client.unsubscribe(request).await?;
            println!("Unsubscribed: {:?}", response.into_inner().success);
        }
        TopicsCommands::Subscriptions { topic } => {
            let _topic = topic;
            // to implement
            todo!();
            let request = TopicRequest { name: topic };
            let response = client.list_subscriptions(request).await?;
            println!("Subscriptions: {:?}", response.into_inner().subscriptions);
        }
        TopicsCommands::CreateSubscription {
            subscription,
            topic,
        } => {
            let _topic = topic;
            let _subscription = subscription;
            // to implement
            todo!();
            let request = SubscriptionRequest {
                topic,
                subscription,
            };
            let response = client.create_subscription(request).await?;
            println!("Subscription Created: {:?}", response.into_inner().success);
        }
    }

    Ok(())
}