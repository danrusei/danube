use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::convert::TryFrom;

use crate::proto::{schema::TypeSchema as ProtoTypeSchema, Schema as ProtoSchema};

// Define the enum with serde attributes for (de)serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaType {
    Bytes,
    String,
    Int64,
    Json(String), // JSON schema described by a string
}

// Implement conversions from SchemaType to ProtoTypeSchema
impl From<SchemaType> for ProtoTypeSchema {
    fn from(schema_type: SchemaType) -> Self {
        match schema_type {
            SchemaType::Bytes => ProtoTypeSchema::Bytes,
            SchemaType::String => ProtoTypeSchema::String,
            SchemaType::Int64 => ProtoTypeSchema::Int64,
            SchemaType::Json(_) => ProtoTypeSchema::Json,
        }
    }
}

// Implement conversions from ProtoTypeSchema to SchemaType
impl From<ProtoTypeSchema> for SchemaType {
    fn from(proto_schema: ProtoTypeSchema) -> Self {
        match proto_schema {
            ProtoTypeSchema::Bytes => SchemaType::Bytes,
            ProtoTypeSchema::String => SchemaType::String,
            ProtoTypeSchema::Int64 => SchemaType::Int64,
            ProtoTypeSchema::Json => SchemaType::Json(String::new()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    name: String,
    schema_data: Option<Vec<u8>>,
    type_schema: SchemaType,
}

impl Schema {
    pub fn new(name: String, type_schema: SchemaType) -> Self {
        let schema_data = match &type_schema {
            SchemaType::Json(schema) => Some(schema.as_bytes().to_vec()),
            _ => None,
        };
        Self {
            name,
            schema_data,
            type_schema,
        }
    }

    // Function to convert Schema to serde_json::Value
    fn schema_to_json_value(proto_schema: &ProtoSchema) -> Value {
        let schema: Schema = proto_schema.into();
        serde_json::to_value(&schema).unwrap()
    }
}

// Implement a conversion method from ProtoSchema to Schema
impl From<&ProtoSchema> for Schema {
    fn from(proto_schema: &ProtoSchema) -> Self {
        let type_schema =
            ProtoTypeSchema::try_from(proto_schema.type_schema).expect("Invalid type schema");
        Schema {
            name: proto_schema.name.clone(),
            schema_data: Some(proto_schema.schema_data.clone()),
            type_schema: type_schema.into(),
        }
    }
}

// Implement a conversion method from Schema to ProtoSchema
impl From<&Schema> for ProtoSchema {
    fn from(schema: &Schema) -> Self {
        ProtoSchema {
            name: schema.name.clone(),
            schema_data: schema.schema_data.clone().unwrap(),
            type_schema: ProtoTypeSchema::from(schema.type_schema.clone()).into(),
        }
    }
}
