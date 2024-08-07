use crate::proto::{
    topic_admin_client::TopicAdminClient, NamespaceRequest, NewTopicRequest, SubscriptionRequest,
    TopicRequest,
};
use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub(crate) struct Topics {
    #[command(subcommand)]
    command: TopicsCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum TopicsCommands {
    #[command(about = "List topics in the specified namespace")]
    List { namespace: String },
    #[command(about = "Create a non-partitioned topic")]
    Create {
        topic: String,
        #[arg(short, long, default_value = "String")]
        schema_type: String,
        #[arg(short = 'd', long, default_value = "{}")]
        schema_data: String,
    },
    #[command(about = "Delete an existing topic")]
    Delete { topic: String },
    #[command(about = "Delete a subscription from a topic")]
    Unsubscribe {
        topic: String,
        #[arg(short, long)]
        subscription: String,
    },
    #[command(about = "List the subscriptions of the specified topic")]
    Subscriptions { topic: String },
    #[command(about = "Create a new subscription for the specified topic")]
    CreateSubscription {
        #[arg(short, long)]
        subscription: String,
        topic: String,
    },
}

#[allow(unreachable_code)]
pub async fn handle_command(topics: Topics) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = TopicAdminClient::connect("http://127.0.0.1:50051").await?;

    match topics.command {
        // Get the list of topics of a namespace
        TopicsCommands::List { namespace } => {
            let request = NamespaceRequest { name: namespace };
            let response = client.list_topics(request).await?;

            let topics = response.into_inner().topics;

            for topic in topics {
                println!("Topic: {}", topic);
            }
        }

        // Creates a non-partitioned topic
        TopicsCommands::Create {
            topic,
            mut schema_type,
            schema_data,
        } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            if schema_type.is_empty() {
                schema_type = "String".into()
            }
            let request = NewTopicRequest {
                name: topic,
                schema_type,
                schema_data,
            };
            let response = client.create_topic(request).await?;
            println!("Topic Created: {:?}", response.into_inner().success);
        }

        // Delete the topic
        TopicsCommands::Delete { topic } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            let request = TopicRequest { name: topic };
            let response = client.delete_topic(request).await?;
            println!("Topic Deleted: {:?}", response.into_inner().success);
        }

        // Delete a subscription from a topic
        TopicsCommands::Unsubscribe {
            topic,
            subscription,
        } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

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
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            let request = TopicRequest { name: topic };
            let response = client.list_subscriptions(request).await?;
            println!("Subscriptions: {:?}", response.into_inner().subscriptions);
        }

        // Create a new subscription for the topic
        TopicsCommands::CreateSubscription {
            subscription,
            topic,
        } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

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

// Topics string representation:  /{namespace}/{topic-name}
pub(crate) fn validate_topic_format(input: &str) -> bool {
    let parts: Vec<&str> = input.split('/').collect();

    if parts.len() != 3 {
        return false;
    }

    for part in parts.iter() {
        if !part
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return false;
        }
    }

    true
}
