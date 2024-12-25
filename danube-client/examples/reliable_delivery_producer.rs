use anyhow::Result;
use danube_client::{ConfigDispatchStrategy, DanubeClient};
use std::fs;
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

    // Read the blob file into memory
    let blob_data = fs::read("./examples/test.blob")?;

    // Create a reliable delivery strategy with a retention period of 1 hour and a segment size of 1 MB
    let dispatch_strategy = ConfigDispatchStrategy::new("reliable", 3600, 1);

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_dispatch_strategy(dispatch_strategy)
        .build();

    producer.create().await?;
    info!("The Producer {} was created", producer_name);

    let mut i = 0;

    while i < 100 {
        let blob_cloned = blob_data.clone();
        match producer.send(blob_cloned, None).await {
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
