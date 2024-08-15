use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use danube_client::{DanubeClient, SchemaType, SubType};
use serde_json::{from_slice, Value};
use std::{collections::HashMap, str::from_utf8};
use valico::json_schema::{self, schema::ScopedSchema};

#[derive(Debug, Parser)]
pub struct Consume {
    #[arg(
        long,
        short = 's',
        help = "The service URL for the Danube broker. Example: http://127.0.0.1:6650"
    )]
    pub service_addr: String,

    #[arg(
        long,
        short = 't',
        default_value = "/default/test_topic",
        help = "The topic to consume messages from"
    )]
    pub topic: String,

    #[arg(
        long,
        short = 'n',
        default_value = "consumer_pubsub",
        help = "The consumer name"
    )]
    pub consumer: String,

    #[arg(long, short = 'm', help = "The subscription name")]
    pub subscription: String,

    #[arg(long, value_enum, help = "The subscription type. Default: Shared")]
    pub sub_type: Option<SubTypeArg>,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum SubTypeArg {
    Exclusive,
    Shared,
    FailOver,
}

pub async fn handle_consume(consume: Consume) -> Result<()> {
    let sub_type = validate_subscription_type(consume.sub_type)?;

    let client = DanubeClient::builder()
        .service_url(&consume.service_addr)
        .build()?;

    let mut consumer = client
        .new_consumer()
        .with_topic(consume.topic.clone())
        .with_consumer_name(consume.consumer)
        .with_subscription(consume.subscription)
        .with_subscription_type(sub_type)
        .build();

    // Retrieve schema type and schema definition
    let schema = client.get_schema(consume.topic).await?;

    let mut scope = json_schema::Scope::new();

    let schema_validator = match schema.type_schema {
        SchemaType::Json(ref schema) => {
            let schema_value: Value = serde_json::from_str(&schema)?;
            Some(scope.compile_and_return(schema_value, true)?)
        }
        _ => None,
    };

    consumer.subscribe().await?;
    let mut message_stream = consumer.receive().await?;

    while let Some(stream_message) = message_stream.recv().await {
        let payload = stream_message.payload;
        let (seq_id, attr) = if let Some(meta) = stream_message.metadata {
            (meta.sequence_id, meta.attributes)
        } else {
            (0, HashMap::new())
        };

        // Process message based on the schema type
        process_message(
            &payload,
            seq_id,
            attr,
            &schema.type_schema,
            &schema_validator,
        )?;
    }

    Ok(())
}

fn process_message(
    payload: &[u8],
    seq: u64,
    attr: HashMap<String, String>,
    schema_type: &SchemaType,
    schema_validator: &Option<ScopedSchema>,
) -> Result<()> {
    match schema_type {
        SchemaType::Bytes => {
            let decoded_message = from_utf8(payload)?;
            print_to_console(seq, decoded_message, attr);
        }
        SchemaType::String => {
            let decoded_message = from_utf8(payload)?;
            print_to_console(seq, decoded_message, attr);
        }
        SchemaType::Int64 => {
            let message = std::str::from_utf8(payload)
                .context("Invalid UTF-8 sequence")?
                .parse::<i64>()
                .context("Failed to parse Int64")?;
            print_to_console(seq, &message.to_string(), attr);
        }
        SchemaType::Json(_) => {
            if let Some(validator) = schema_validator {
                process_json_message(payload, validator)?;

                // If validation passes, handle the JSON message
                let json_str = from_utf8(payload).context("Invalid UTF-8 sequence")?;
                print_to_console(seq, json_str, attr);
            } else {
                eprintln!("JSON schema validator is missing.");
            }
        }
    }
    Ok(())
}

fn process_json_message(payload: &[u8], schema_validator: &ScopedSchema) -> Result<()> {
    let json_value: Value = from_slice(payload)?;
    // Validate the JSON message against the schema
    if !schema_validator.validate(&json_value).is_valid() {
        eprintln!("JSON message validation failed: {}", json_value);
        return Ok(()); // Continue processing other messages even if validation fails
    }

    Ok(())
}

fn validate_subscription_type(subscription_type: Option<SubTypeArg>) -> Result<SubType> {
    let sub_type = if let Some(subcr_type) = subscription_type {
        if SubTypeArg::value_variants().contains(&subcr_type) {
            subcr_type.into()
        } else {
            return Err(anyhow::anyhow!(
                "Unsupported subscription type: '{:?}'. Supported values are: {:?}",
                subcr_type,
                SubTypeArg::value_variants()
            )
            .into());
        }
    } else {
        SubType::Shared
    };

    Ok(sub_type)
}

impl From<SubTypeArg> for SubType {
    fn from(arg: SubTypeArg) -> Self {
        match arg {
            SubTypeArg::Exclusive => SubType::Exclusive,
            SubTypeArg::Shared => SubType::Shared,
            SubTypeArg::FailOver => SubType::FailOver,
        }
    }
}

fn print_attr(attributes: &HashMap<String, String>) -> String {
    let formatted: Vec<String> = attributes
        .iter()
        .map(|(key, value)| format!("{}={}", key, value))
        .collect();

    let result = formatted.join(", ");
    result
}

fn print_to_console(seq: u64, message: &str, attributes: HashMap<String, String>) {
    if attributes.is_empty() {
        println!("Received bytes message: {}, with payload: {}", seq, message);
    } else {
        println!(
            "Received bytes message: {}, with payload: {}, with attributes: {}",
            seq,
            message,
            print_attr(&attributes)
        );
    }
}
