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

    consumer.subscribe().await?;

    // while let Some(message) = consumer.receive().await? {
    //     println!("Received message: {:?}", message);

    //     // Acknowledge the message
    // }

    Ok(())
}
