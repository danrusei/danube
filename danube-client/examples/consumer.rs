use anyhow::Result;
use danube_client::{DanubeClient, SchemaType, SubType};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let client = DanubeClient::builder()
        .service_url("http://[::1]:6650")
        .build()
        .unwrap();

    let topic = env::var("DANUBE_TOPIC")
        .ok()
        .unwrap_or_else(|| "public_test".to_string());

    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name("test_consumer")
        .with_subscription("test_subscription")
        .with_subscription_type(SubType::Exclusive)
        .build();

    // Subscribe to the topic
    consumer.subscribe().await?;

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    while let Some(message) = message_stream.next_message().await {
        // Process the message
        println!("Received message: {:?}", message);

        // Acknowledge the message or perform other actions
    }

    Ok(())
}
