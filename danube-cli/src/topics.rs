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
        // Get the list of topics of a namespace
        TopicsCommands::List { namespace } => {
            let _namespace = namespace;
            // to implement
            todo!();
            let request = NamespaceRequest { name: namespace };
            let response = client.list_topics(request).await?;
            println!("Topics: {:?}", response.into_inner().topics);
        }
        // Creates a non-partitioned topic
        TopicsCommands::Create { topic } => {
            let _topic = topic;
            // to implement
            todo!();
            let request = TopicRequest { name: topic };
            let response = client.create_topic(request).await?;
            println!("Topic Created: {:?}", response.into_inner().success);
        }
        // Create a partitioned topic (--partitions #)
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
        // Delete the topic
        TopicsCommands::Delete { topic } => {
            let _topic = topic;
            // to implement
            todo!();
            let request = TopicRequest { name: topic };
            let response = client.delete_topic(request).await?;
            println!("Topic Deleted: {:?}", response.into_inner().success);
        }
        // Delete a subscription from a topic
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
        // Get the list of subscriptions on the topic
        TopicsCommands::Subscriptions { topic } => {
            let _topic = topic;
            // to implement
            todo!();
            let request = TopicRequest { name: topic };
            let response = client.list_subscriptions(request).await?;
            println!("Subscriptions: {:?}", response.into_inner().subscriptions);
        }
        // Create a new subscription for the topic
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
