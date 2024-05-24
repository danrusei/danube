use anyhow::Result;
use danube_client::{DanubeClient, SchemaType};
use serde_json::json;
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

    let json_schema = r#"{"type": "object", "properties": {"field1": {"type": "string"}, "field2": {"type": "integer"}}}"#.to_string();

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name("test_producer")
        .with_schema("my_app".into(), SchemaType::Json(json_schema))
        .build();

    producer.create().await?;

    let mut i = 0;

    while i < 10 {
        let data = json!({
            "field1": format!{"value{}", i},
            "field2": 2020+i,
        });

        // Convert to string and encode to bytes
        let json_string = serde_json::to_string(&data).unwrap();
        let encoded_data = json_string.as_bytes().to_vec();

        // let json_message = r#"{"field1": "value", "field2": 123}"#.as_bytes().to_vec();
        producer.send(encoded_data).await?;
        i += 1;
    }

    Ok(())
}
