use anyhow::Result;
use clap::{Parser, ValueEnum};
use danube_client::{DanubeClient, SchemaType};
use tokio::time::{sleep, Duration};

#[derive(Debug, Parser)]
pub struct Produce {
    #[arg(
        long,
        short = 'a',
        help = "The service URL for the Danube broker. Example: http://127.0.0.1:6650"
    )]
    pub service_addr: String,

    #[arg(
        long,
        short = 't',
        default_value = "/default/test_topic",
        help = "The topic to produce messages to."
    )]
    pub topic: String,

    #[arg(
        long,
        short = 's',
        value_enum,
        help = "The schema type of the message."
    )]
    pub schema: Option<SchemaTypeArg>,

    #[arg(
        long,
        short = 'm',
        help = "The message to send. This is a required argument."
    )]
    pub message: String,

    #[arg(long, help = "The JSON schema, required if schema type is Json.")]
    pub json_schema: Option<String>,

    #[arg(
        long,
        short = 'c',
        default_value = "1",
        help = "Number of times to send the message."
    )]
    pub count: u32,

    #[arg(
        long,
        short = 'i',
        default_value = "500",
        help = "Interval between messages in milliseconds. Default: 500. Minimum: 100."
    )]
    pub interval: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum SchemaTypeArg {
    Bytes,
    String,
    Int64,
    Json,
}

pub async fn handle_produce(produce: Produce) -> Result<()> {
    // Validate interval
    if produce.interval < 100 {
        return Err(anyhow::anyhow!("The interval must be at least 100 milliseconds").into());
    }

    let client = DanubeClient::builder()
        .service_url(&produce.service_addr)
        .build()?;

    let schema_type = validate_schema(produce.schema, produce.json_schema)?;

    let mut producer = client
        .new_producer()
        .with_topic(produce.topic)
        .with_name("test_producer")
        .with_schema("my_app".into(), schema_type) // Pass the correct schema type
        .build();

    let _ = producer.create().await?;

    let encoded_data = produce.message.as_bytes().to_vec();

    for _ in 0..produce.count {
        let message_id = producer.send(encoded_data.clone()).await?;
        println!("The Message with ID {} was sent", message_id);
        if produce.count - 1 > 0 {
            sleep(Duration::from_millis(produce.interval)).await;
        }
    }

    Ok(())
}

impl From<SchemaTypeArg> for SchemaType {
    fn from(arg: SchemaTypeArg) -> Self {
        match arg {
            SchemaTypeArg::Bytes => SchemaType::Bytes,
            SchemaTypeArg::String => SchemaType::String,
            SchemaTypeArg::Int64 => SchemaType::Int64,
            SchemaTypeArg::Json => SchemaType::Json(String::new()), // Placeholder
        }
    }
}

fn validate_schema(
    schema_type: Option<SchemaTypeArg>,
    json_schema: Option<String>,
) -> Result<SchemaType> {
    if let Some(schema_type) = schema_type {
        if !SchemaTypeArg::value_variants().contains(&schema_type) {
            return Err(anyhow::anyhow!(
                "Unsupported schema type: '{:?}'. Supported values are: {:?}",
                schema_type,
                SchemaTypeArg::value_variants()
            )
            .into());
        }
    }

    match schema_type {
        Some(schema_type) => match schema_type {
            SchemaTypeArg::Json => {
                if let Some(json_schema) = json_schema {
                    return Ok(SchemaType::Json(json_schema));
                } else {
                    return Err(
                        anyhow::anyhow!("JSON schema is required for schema type 'Json'").into(),
                    );
                }
            }
            _ => return Ok(schema_type.into()),
        },
        None => return Ok(SchemaTypeArg::String.into()),
    };
}
