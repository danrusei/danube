use base64::prelude::*;
use prost::Message;
use thiserror::Error;
use tonic::{codegen::http::uri, metadata::MetadataValue, Status};

use crate::proto::{ErrorMessage, ErrorType};

pub type Result<T> = std::result::Result<T, DanubeError>;

#[derive(Debug, Error)]
pub enum DanubeError {
    #[error("transport error: {0}")]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error("from status error: {0}, with message: {1:?}")]
    FromStatus(tonic::Status, Option<ErrorMessage>),

    #[error("unable to parse the address: {0}")]
    UrlParseError(#[from] uri::InvalidUri),

    #[error("unable to parse the address")]
    ParseError,

    #[error("unable to perform operation: {0}")]
    Unrecoverable(String),
}

pub(crate) fn decode_error_details(status: &Status) -> Option<ErrorMessage> {
    if let Some(metadata_value) = status.metadata().get_bin("error-message-bin") {
        let base64_buffer = metadata_value.as_encoded_bytes();

        // Decode the base64 string to get the original bytes
        let buffer = BASE64_STANDARD_NO_PAD.decode(base64_buffer).unwrap();

        // Decode the protobuf message directly from the metadata bytes
        match ErrorMessage::decode(&buffer[..]) {
            Ok(error_message) => Some(error_message),
            Err(err) => {
                eprintln!("Error decoding error message: {}", err);
                None
            }
        }
    } else {
        None
    }
}

// A helper function to convert i32 to ErrorType.
pub(crate) fn error_type_from_i32(value: i32) -> Option<ErrorType> {
    match value {
        0 => Some(ErrorType::UnknownError),
        1 => Some(ErrorType::InvalidTopicName),
        2 => Some(ErrorType::TopicNotFound),
        3 => Some(ErrorType::ServiceNotReady),
        4 => Some(ErrorType::ProducerAlreadyExists),
        5 => Some(ErrorType::SubscribePermissionDenied),
        6 => Some(ErrorType::SubscriptionNotFound),
        _ => None,
    }
}
