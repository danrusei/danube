use anyhow::Result;
use danube_client::{proto::Schema, DanubeClient, ProducerOptions};
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

    let producer = client
        .new_producer()
        .with_topic(topic)
        .with_name("test_producer")
        .with_options(ProducerOptions {
            schema: Some(Schema {
                name: "schema_name".to_string(),
                schema_data: "1".as_bytes().to_vec(),
                type_schema: 0,
            }),
        })
        .build();

    producer.create().await?;

    Ok(())
}
