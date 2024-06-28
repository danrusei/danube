# Danube-client

An async Rust client library for interacting with Danube Pub/Sub messaging platform.

[Danube](https://github.com/danrusei/danube) is an open-source **distributed** Pub/Sub messaging platform written in Rust.

**⚠️ This library is currently under active development and may have missing or incomplete functionalities. Use with caution.**

I'm working on improving and adding new features. Please feel free to contribute or report any issues you encounter.

## Example usage

Check out the [example files](https://github.com/danrusei/danube/tree/main/danube-client/examples).

### Producer

```rust
use anyhow::Result;
use danube_client::{DanubeClient, SchemaType};
use serde_json::json;
use std::thread;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing
    tracing_subscriber::fmt::init();

    let client = DanubeClient::builder()
        .service_url("http://[::1]:6650")
        .build()
        .unwrap();

    let topic = "/default/test_topic".to_string();

    let json_schema = r#"{"type": "object", "properties": {"field1": {"type": "string"}, "field2": {"type": "integer"}}}"#.to_string();

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name("test_producer1")
        .with_schema("my_app".into(), SchemaType::Json(json_schema))
        .build();

    let prod_id = producer.create().await?;
    info!("The Producer was created with ID: {:?}", prod_id);

    let mut i = 0;

    while i < 20 {
        let data = json!({
            "field1": format!{"value{}", i},
            "field2": 2020+i,
        });

        // Convert to string and encode to bytes
        let json_string = serde_json::to_string(&data).unwrap();
        let encoded_data = json_string.as_bytes().to_vec();

        // let json_message = r#"{"field1": "value", "field2": 123}"#.as_bytes().to_vec();
        let message_id = producer.send(encoded_data).await?;
        println!("The Message with id {} was sent", message_id);

        thread::sleep(Duration::from_secs(1));
        i += 1;
    }

    Ok(())
}
```

### Consumer

```rust
use anyhow::Result;
use danube_client::{DanubeClient, SubType};
use futures_util::stream::StreamExt;
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
```
