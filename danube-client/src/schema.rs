use crate::proto::{schema::TypeSchema, Schema as ProtoSchema};

#[derive(Debug, Clone)]
pub enum SchemaType {
    Bytes,
    String,
    Int64,
    Json(String), // JSON schema described by a string
}

impl From<SchemaType> for TypeSchema {
    fn from(schema_type: SchemaType) -> Self {
        match schema_type {
            SchemaType::Bytes => TypeSchema::Bytes,
            SchemaType::String => TypeSchema::String,
            SchemaType::Int64 => TypeSchema::Int64,
            SchemaType::Json(_) => TypeSchema::Json,
        }
    }
}

#[derive(Debug, Clone)]
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

    pub fn to_proto(&self) -> ProtoSchema {
        ProtoSchema {
            name: self.name.clone(),
            schema_data: self.schema_data.clone().unwrap_or_default(),
            type_schema: TypeSchema::from(self.type_schema.clone()) as i32,
        }
    }
}
