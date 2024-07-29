use anyhow::Result;
use clap::Parser;
use danube_client::{DanubeClient, SchemaType};

#[derive(Debug, Parser)]
pub struct Produce {
    #[arg(long, help = "The service URL for the Danube broker")]
    pub service_url: String,

    #[arg(long, help = "The topic to produce messages to")]
    pub topic: String,

    #[arg(long, help = "The schema type of the message")]
    pub schema: String,

    #[arg(long, help = "The message to send")]
    pub message: String,
}

pub async fn handle_produce(produce: Produce) -> Result<()> {
    let client = DanubeClient::builder()
        .service_url(&produce.service_url)
        .build()?;

    let mut producer = client
        .new_producer()
        .with_topic(produce.topic)
        .with_name("test_producer")
        .with_schema("my_app".into(), SchemaType::Json(produce.schema))
        .build();

    producer.create().await?;

    let encoded_data = produce.message.as_bytes().to_vec();
    let message_id = producer.send(encoded_data).await?;
    println!("The Message with ID {} was sent", message_id);

    Ok(())
}
