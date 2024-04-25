use thiserror::Error;
use tonic::codegen::http::uri;

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
