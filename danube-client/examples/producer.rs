use anyhow::Result;
use danube_client::DanubeClient;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let client = DanubeClient::builder()
        .service_url("http://[::1]:6650")
        .build();
    client.connect().await?;

    let topic = env::var("DANUBE_TOPIC")
        .ok()
        .unwrap_or_else(|| "non-persistent://public/test".to_string());

    let producer = client
        .new_producer()
        .with_topic(topic)
        .with_name("test producer")
        .create();

    Ok(())
}
