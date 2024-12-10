use crate::proto::{MessageMetadata as ProtoMessageMeta, StreamMessage};
use std::collections::HashMap;

#[derive(Clone, PartialEq)]
pub struct ConsumerMessage {
    pub request_id: u64,
    pub message_id: u64,
    pub payload: Vec<u8>,
    pub attributes: Option<HashMap<String, String>>,
}
impl ConsumerMessage {
    // Helper function to convert generated structs to local structs
    pub fn from_proto(message: StreamMessage) -> Self {
        todo!();
    }
}
