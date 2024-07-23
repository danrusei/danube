extern crate danube_client;
extern crate futures_util;

use anyhow::Result;
use danube_client::{Consumer, DanubeClient, Producer, SchemaType, SubType};
use futures_util::stream::StreamExt;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

struct TestSetup {
    client: Arc<DanubeClient>,
}

async fn setup() -> Result<TestSetup> {
    let client = Arc::new(
        DanubeClient::builder()
            .service_url("http://127.0.0.1:6650")
            .build()
            .unwrap(),
    );

    Ok(TestSetup { client })
}

async fn setup_producer(
    client: Arc<DanubeClient>,
    topic: &str,
    producer_name: &str,
) -> Result<Producer> {
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema("my_schema".into(), SchemaType::String)
        .build();

    producer.create().await?;

    Ok(producer)
}

async fn setup_consumer(
    client: Arc<DanubeClient>,
    topic: &str,
    consumer_name: &str,
    sub_type: SubType,
) -> Result<Consumer> {
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.to_string())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(format!("test_subscription_{}", consumer_name))
        .with_subscription_type(sub_type)
        .build();

    // Ensure the consumer is connected and subscribed
    consumer.subscribe().await?;
    Ok(consumer)
}

#[tokio::test]
async fn test_exclusive_subscription() -> Result<()> {
    let setup = setup().await?;
    let topic = "/default/topic_test_exclusive_subscription";

    let producer = setup_producer(setup.client.clone(), topic, "test_producer_exclusive").await?;

    let mut consumer = setup_consumer(
        setup.client.clone(),
        topic,
        "test_consumer_exclusive",
        SubType::Exclusive,
    )
    .await?;

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    // Produce a message after the consumer has subscribed
    let _message_id = producer.send("Hello Danube".as_bytes().into()).await?;
    println!("Message sent");

    // Add a timeout to avoid blocking indefinitely
    let receive_future = async {
        if let Some(message) = message_stream.next().await {
            let msg = message.unwrap();
            let payload = String::from_utf8(msg.messages).unwrap();
            assert_eq!(payload, "Hello Danube");
            println!("Message received: {}", payload);
            // consumer.ack(&msg).await.unwrap();
        } else {
            println!("No message received");
        }
    };

    let result = timeout(Duration::from_secs(10), receive_future).await;
    assert!(
        result.is_ok(),
        "Test timed out while waiting for the message"
    );

    Ok(())
}

#[tokio::test]
async fn test_shared_subscription() -> Result<()> {
    let setup = setup().await?;
    let topic = "/default/topic_test_shared_subscription";

    let producer = setup_producer(setup.client.clone(), topic, "test_producer_shared").await?;

    let mut consumer = setup_consumer(
        setup.client.clone(),
        topic,
        "test_consumer_shared",
        SubType::Shared,
    )
    .await?;

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    // Produce a message after the consumer has subscribed
    let _message_id = producer.send("Hello Danube".as_bytes().into()).await?;
    println!("Message sent");

    // Add a timeout to avoid blocking indefinitely
    let receive_future = async {
        if let Some(message) = message_stream.next().await {
            let msg = message.unwrap();
            let payload = String::from_utf8(msg.messages).unwrap();
            assert_eq!(payload, "Hello Danube");
            println!("Message received: {}", payload);
            // consumer.ack(&msg).await.unwrap();
        } else {
            println!("No message received");
        }
    };

    let result = timeout(Duration::from_secs(10), receive_future).await;
    assert!(
        result.is_ok(),
        "Test timed out while waiting for the message"
    );

    Ok(())
}
