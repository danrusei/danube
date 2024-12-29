use serde_json;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, MetadataError>;

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Watch stream error: {0}")]
    WatchError(String),

    #[error("Storage backend error: {0}")]
    StorageError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Failed to connect to storage: {0}")]
    ConnectionError(String),

    #[error("Watch operation timed out")]
    WatchTimeout,

    #[error("Watch channel closed")]
    WatchChannelClosed,

    #[error("Invalid watch key: {0}")]
    InvalidWatchKey(String),

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
        MetadataError::StorageError(Box::new(err))
    }
}
