use anyhow::Result;
use clap::{Parser, ValueEnum};
use danube_client::{DanubeClient, SchemaType};

#[derive(Debug, Parser)]
pub struct Produce {
    #[arg(long, short = 'a', help = "The service URL for the Danube broker")]
    pub service_url: String,

    #[arg(
        long,
        short = 't',
        default_value = "/default/default",
        help = "The topic to produce messages to"
    )]
    pub topic: String,

    #[arg(long, short = 's', value_enum, help = "The schema type of the message")]
    pub schema: Option<SchemaTypeArg>,

    #[arg(long, short = 'm', help = "The message to send")]
    pub message: String,

    #[arg(long, help = "The JSON schema, required if schema type is Json")]
    pub json_schema: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum SchemaTypeArg {
    Bytes,
    String,
    Int64,
    Json,
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

pub async fn handle_produce(produce: Produce) -> Result<()> {
    let client = DanubeClient::builder()
        .service_url(&produce.service_url)
        .build()?;

    let schema_type = validate_schema(produce.schema, produce.json_schema)?;

    let mut producer = client
        .new_producer()
        .with_topic(produce.topic)
        .with_name("test_producer")
        .with_schema("my_app".into(), schema_type) // Pass the correct schema type
        .build();

    producer.create().await?;

    let encoded_data = produce.message.as_bytes().to_vec();
    let message_id = producer.send(encoded_data).await?;
    println!("The Message with ID {} was sent", message_id);

    Ok(())
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
