use anyhow::Result;
use danube_client::{DanubeClient, SubType};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct MyMessage {
    field1: String,
    field2: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing
    tracing_subscriber::fmt::init();

    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .unwrap();

    let topic = "/default/partitioned_topic";
    let consumer_name = "cons_part";
    let subscription_name = "subs_part";

    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name(consumer_name)
        .with_subscription(subscription_name)
        .with_subscription_type(SubType::Exclusive)
        .build();

    // Subscribe to the topic
    let consumer_id = consumer.subscribe().await?;
    println!("The Consumer with ID: {:?} was created", consumer_id);

    let _schema = client.get_schema(topic).await.unwrap();

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    while let Some(message) = message_stream.recv().await {
        let payload = message.payload;
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

    Ok(())
}
