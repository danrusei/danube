use crate::proto::{MessageMetadata as ProtoMessageMeta, MessageRequest};

#[derive(Clone, PartialEq)]
pub struct MessageMetadata {
    pub producer_name: String,
    pub sequence_id: u64,
    pub publish_time: u64,
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
            }),
            None => None,
        };

        MessageRequest {
            request_id: self.request_id,
            producer_id: self.producer_id,
            metadata,
            message: self.message.clone(),
        }
    }
}
