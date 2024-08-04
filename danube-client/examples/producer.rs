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
        let message_id = producer.send(encoded_data, None).await?;
        println!("The Message with id {} was sent", message_id);

        thread::sleep(Duration::from_secs(1));
        i += 1;
    }

    Ok(())
}
