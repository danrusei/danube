extern crate danube_client;
extern crate futures_util;

use anyhow::Result;
use danube_client::{Consumer, DanubeClient, Producer, SchemaType, SubType};
use futures_util::stream::StreamExt;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};

struct TestSetup {
    producer: Arc<Mutex<Producer>>,
    client: Arc<DanubeClient>,
    topic: String,
}

static CLIENT: OnceCell<Arc<DanubeClient>> = OnceCell::new();
static PRODUCER: OnceCell<Arc<Mutex<Producer>>> = OnceCell::new();

async fn setup() -> Result<TestSetup> {
    let client = CLIENT
        .get_or_init(|| {
            Arc::new(
                DanubeClient::builder()
                    .service_url("http://[::1]:6650")
                    .build()
                    .unwrap(),
            )
        })
        .clone();

    let topic = "/default/test_topic".to_string();

    // Initialize the producer only once
    if PRODUCER.get().is_none() {
        let mut producer = client
            .new_producer()
            .with_topic(topic.clone())
            .with_name("test_producer1")
            .with_schema("my_app".into(), SchemaType::String)
            .build();

        producer.create().await?;

        PRODUCER.set(Arc::new(Mutex::new(producer))).unwrap();
    }

    let producer = PRODUCER.get().unwrap().clone();

    Ok(TestSetup {
        producer,
        client,
        topic,
    })
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

    let mut consumer = setup_consumer(
        setup.client.clone(),
        &setup.topic,
        "test_consumer_exclusive",
        SubType::Exclusive,
    )
    .await?;
    let producer = setup.producer.lock().await;

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
    // find a better way to wait for initial setup
    sleep(Duration::from_secs(4)).await;

    let setup = setup().await?;

    let mut consumer = setup_consumer(
        setup.client.clone(),
        &setup.topic,
        "test_consumer_shared",
        SubType::Shared,
    )
    .await?;
    let producer = setup.producer.lock().await;

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
