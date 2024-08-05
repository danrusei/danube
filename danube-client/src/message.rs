use std::collections::HashMap;

use crate::proto::{MessageMetadata as ProtoMessageMeta, MessageRequest};

#[derive(Clone, PartialEq)]
pub struct MessageMetadata {
    // Optional, identifies the producer’s name
    pub producer_name: String,
    // Optional, useful for maintaining order and deduplication
    pub sequence_id: u64,
    // Optional, timestamp for when the message was published
    pub publish_time: u64,
    // Optional, user-defined properties/attributes
    pub attributes: HashMap<String, String>,
}

#[derive(Clone, PartialEq)]
pub struct SendMessage {
    pub request_id: u64,
    pub producer_id: u64,
    pub metadata: Option<MessageMetadata>,
    pub message: Vec<u8>,
}

impl SendMessage {
    // Helper function to convert local structs to generated structs
    pub fn to_proto(&self) -> MessageRequest {
        let metadata = match &self.metadata {
            Some(meta) => Some(ProtoMessageMeta {
                producer_name: meta.producer_name.clone(),
                sequence_id: meta.sequence_id,
                publish_time: meta.publish_time,
                attributes: meta.attributes.clone(),
            }),
            None => None,
        };

        MessageRequest {
            request_id: self.request_id,
            producer_id: self.producer_id,
            metadata,
            payload: self.message.clone(),
        }
    }
}
