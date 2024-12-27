use danube_reliable_dispatch::ReliableDispatch;
use danube_reliable_dispatch::StorageBackendType;
use serde::{Deserialize, Serialize};

use crate::proto::TopicDispatchStrategy;

#[derive(Debug)]
pub(crate) enum DispatchStrategy {
    // Does not store messages, sends them directly to the dispatcher
    NonReliable,
    // Stores messages in a queue for reliable delivery
    // TODO! - ensure that the messages are delivered in order and are acknowledged before removal from the queue
    // TODO! - TTL - implement a retention policy to remove messages from the queue after a certain period of time (e.g. 1 hour)
    Reliable(ReliableDispatch),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ConfigDispatchStrategy {
    pub(crate) strategy: String,
    pub(crate) retention_period: u64,
    pub(crate) segment_size: usize,
    pub(crate) storage_type: StorageBackendType,
}

impl Default for ConfigDispatchStrategy {
    fn default() -> Self {
        ConfigDispatchStrategy {
            strategy: "non_reliable".to_string(),
            retention_period: 3600,
            segment_size: 50,
            storage_type: StorageBackendType::InMemory,
        }
    }
}

// Implement conversions from ProtoTypeSchema to SchemaType
impl From<TopicDispatchStrategy> for ConfigDispatchStrategy {
    fn from(strategy: TopicDispatchStrategy) -> Self {
        let storage_type = match strategy.storage_backend {
            0 => StorageBackendType::InMemory,
            1 => StorageBackendType::Disk(strategy.storage_path),
            2 => StorageBackendType::S3(strategy.storage_path),
            _ => StorageBackendType::InMemory,
        };
        ConfigDispatchStrategy {
            strategy: strategy.strategy,
            retention_period: strategy.retention_period,
            segment_size: strategy.segment_size as usize,
            storage_type,
        }
    }
}
