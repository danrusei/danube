extern crate danube_client;
extern crate futures_util;

use anyhow::Result;
use danube_client::{DanubeClient, SchemaType, SubType};
use futures_util::stream::StreamExt;

#[tokio::test]
async fn test_exclusive_subscription() -> Result<()> {
    let client = DanubeClient::builder()
        .service_url("http://[::1]:6650")
        .build()
        .unwrap();

    let topic = "/default/test_topic".to_string();

    // create the producer
    let mut producer = client
        .new_producer()
        .with_topic(topic.clone())
        .with_name("test_producer1")
        .with_schema("my_app".into(), SchemaType::String)
        .build();

    let _prod_id = producer.create().await?;

    let _message_id = producer.send("Hello Danube".as_bytes().into()).await?;

    // Create consumer with exclusive subscription
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("test_consumer")
        .with_subscription("test_subscription")
        .with_subscription_type(SubType::Exclusive)
        .build();

    // Subscribe to the topic
    let _consumer_id = consumer.subscribe().await?;

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    if let Some(message) = message_stream.next().await {
        let msg = message.unwrap();
        let payload = String::from_utf8(msg.messages).unwrap();
        assert_eq!(payload, "Hello Danube");
        // consumer.ack(&msg).await.unwrap();
    }

    Ok(())
}
