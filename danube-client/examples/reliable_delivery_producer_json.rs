use anyhow::Result;
use danube_client::{ConfigDispatchStrategy, DanubeClient, SchemaType};
use serde_json::json;
use std::thread;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing
    tracing_subscriber::fmt::init();

    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .unwrap();

    let topic = "/default/reliable_topic";
    let producer_name = "prod_json_reliable";

    let json_schema = r#"{"type": "object", "properties": {"field1": {"type": "string"}, "field2": {"type": "integer"}}}"#.to_string();

    // Create a reliable delivery strategy with a retention period of 1 hour and a segment size of 10 MB
    let dispatch_strategy = ConfigDispatchStrategy::new("reliable", 3600, 10);

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema("my_app".into(), SchemaType::Json(json_schema))
        .with_dispatch_strategy(dispatch_strategy)
        .build();

    producer.create().await?;
    info!("The Producer {} was created", producer_name);

    let mut i = 0;

    while i < 100 {
        let data = json!({
            "field1": format!{"value{}", i},
            "field2": 2020+i,
        });

        // Convert to string and encode to bytes
        let json_string = serde_json::to_string(&data).unwrap();
        let encoded_data = json_string.as_bytes().to_vec();

        // Send the message and wait for acknowledgment
        match producer.send(encoded_data, None).await {
            Ok(message_id) => {
                println!("The Message with id {} was sent", message_id);
            }
            Err(e) => {
                eprintln!("Failed to send message: {}", e);
            }
        }

        thread::sleep(Duration::from_secs(1));
        i += 1;
    }

    Ok(())
}
