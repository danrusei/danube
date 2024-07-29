use anyhow::Result;
use clap::Parser;
use danube_client::{DanubeClient, SubType};
use futures_util::stream::StreamExt;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct MyMessage {
    field1: String,
    field2: i32,
}

#[derive(Debug, Parser)]
pub struct Consume {
    #[arg(long, help = "The service URL for the Danube broker")]
    pub service_url: String,

    #[arg(long, help = "The topic to consume messages from")]
    pub topic: String,

    #[arg(long, help = "The subscription name")]
    pub subscription: String,

    #[arg(long, help = "The subscription type (Exclusive, Shared, FailOver)")]
    pub sub_type: String,
}

pub async fn handle_consume(consume: Consume) -> Result<()> {
    let sub_type = match consume.sub_type.to_lowercase().as_str() {
        "exclusive" => SubType::Exclusive,
        "shared" => SubType::Shared,
        "failover" => SubType::FailOver,
        _ => return Err(anyhow::anyhow!("Invalid subscription type").into()),
    };

    let client = DanubeClient::builder()
        .service_url(&consume.service_url)
        .build()?;

    let mut consumer = client
        .new_consumer()
        .with_topic(consume.topic)
        .with_consumer_name("test_consumer")
        .with_subscription(consume.subscription)
        .with_subscription_type(sub_type)
        .build();

    consumer.subscribe().await?;
    let mut message_stream = consumer.receive().await?;

    while let Some(message) = message_stream.next().await {
        match message {
            Ok(stream_message) => {
                let payload = stream_message.messages;
                match serde_json::from_slice::<MyMessage>(&payload) {
                    Ok(decoded_message) => println!("Received message: {:?}", decoded_message),
                    Err(e) => eprintln!("Failed to decode message: {}", e),
                }
            }
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
        }
    }

    Ok(())
}
