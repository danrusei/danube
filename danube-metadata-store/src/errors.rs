use serde_json;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, MetadataError>;

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Watch stream error: {0}")]
    WatchError(String),

    #[error("Storage backend error: {0}")]
    StorageError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Transport error: {0}")]
    TransportError(String),

    #[error("Watch operation timed out")]
    WatchTimeout,

    #[error("Watch channel closed")]
    WatchChannelClosed,

    #[error("Lease keepalive error: {0}")]
    LeaseKeepAliveError(String),

    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),

    #[error("Key already exists: {0}")]
    KeyExists(String),

    #[error("Watch cancelled")]
    WatchCancelled,

    #[error("Operation not supported by backend")]
    UnsupportedOperation,

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Unknown error occurred: {0}")]
    Unknown(String),
}

// Convenience impl for etcd errors
impl From<etcd_client::Error> for MetadataError {
    fn from(err: etcd_client::Error) -> Self {
        match err {
            etcd_client::Error::WatchError(msg) => MetadataError::WatchError(msg),
            etcd_client::Error::LeaseKeepAliveError(msg) => MetadataError::LeaseKeepAliveError(msg),
            etcd_client::Error::TransportError(e) => MetadataError::TransportError(e.to_string()),
            etcd_client::Error::InvalidArgs(msg) => MetadataError::InvalidArguments(msg),
            err => MetadataError::StorageError(Box::new(err)),
        }
    }
}
