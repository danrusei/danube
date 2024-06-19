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
}

fn decode_error_details(status: &Status) -> Option<ErrorMessage> {
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
