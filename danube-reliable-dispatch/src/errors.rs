use thiserror::Error;

pub type Result<T> = std::result::Result<T, ReliableDispatchError>;

#[derive(Debug, Error)]
pub enum ReliableDispatchError {
    #[error("Lock acquisition failed: {0}")]
    LockError(String),

    #[error("Segment error: {0}")]
    SegmentError(String),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Acknowledgment error: {0}")]
    AcknowledgmentError(String),

    #[error("Subscription error: {0}")]
    SubscriptionError(String),
}
