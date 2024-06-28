use anyhow::Result;
use danube_client::{DanubeClient, SchemaType, SubType};
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Deserialize, Debug)]
struct MyMessage {
    field1: String,
    field2: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing
    tracing_subscriber::fmt::init();

    let client = DanubeClient::builder()
        .service_url("http://[::1]:6650")
        .build()
        .unwrap();

    let topic = "/default/test_topic".to_string();

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("test_consumer")
        .with_subscription("test_subscription")
        .with_subscription_type(SubType::Exclusive)
        .build();

    // Subscribe to the topic
    let consumer_id = consumer.subscribe().await?;
    println!("The Consumer with ID: {:?} was created", consumer_id);

    let _schema = client.get_schema(topic).await.unwrap();

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    while let Some(message) = message_stream.next().await {
        match message {
            Ok(stream_message) => {
                let payload = stream_message.messages;
                // Deserialize the message using the schema
                match serde_json::from_slice::<MyMessage>(&payload) {
                    Ok(decoded_message) => {
                        println!("Received message: {:?}", decoded_message);
                    }
                    Err(e) => {
                        eprintln!("Failed to decode message: {}", e);
                    }
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
