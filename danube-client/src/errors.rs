use prost::Message;
use thiserror::Error;
use tonic::{codegen::http::uri, metadata::MetadataValue, Status};

use crate::proto::{ErrorMessage, ErrorType};

pub type Result<T> = std::result::Result<T, DanubeError>;

#[derive(Debug, Error)]
pub enum DanubeError {
    #[error("transport error: {0}")]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error("from status error: {0}")]
    FromStatus(#[from] tonic::Status),

    #[error("unable to parse the address: {0}")]
    UrlParseError(#[from] uri::InvalidUri),

    #[error("unable to parse the address")]
    ParseError,

    #[error("unable to perform operation: {0}")]
    Unrecoverable(String),
}

pub(crate) fn decode_error_details(status: &Status) -> Option<ErrorMessage> {
    if let Some(metadata_value) = status.metadata().get_bin("error-details-bin") {
        // Decode the protobuf message directly from the metadata bytes
        match ErrorMessage::decode(metadata_value.as_encoded_bytes()) {
            Ok(error_details) => Some(error_details),
            Err(_) => None,
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
