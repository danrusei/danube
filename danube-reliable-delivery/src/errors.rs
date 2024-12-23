use thiserror::Error;

pub type Result<T> = std::result::Result<T, ReliableDeliveryError>;

#[derive(Debug, Error)]
pub enum ReliableDeliveryError {}
