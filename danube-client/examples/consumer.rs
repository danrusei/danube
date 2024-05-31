use anyhow::Result;
use danube_client::{DanubeClient, SchemaType, SubType};
use futures_util::stream::StreamExt;
use std::env;

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
        .with_topic(topic)
        .with_consumer_name("test_consumer")
        .with_subscription("test_subscription")
        .with_subscription_type(SubType::Exclusive)
        .build();

    // Subscribe to the topic
    let consumer_id = consumer.subscribe().await?;
    println!("The Consumer with ID: {:?} was created", consumer_id);

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    while let Some(message) = message_stream.next().await {
        match message {
            Ok(stream_message) => {
                println!("Received message: {:?}", stream_message.messages);
            }
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
        }
    }

    Ok(())
}
