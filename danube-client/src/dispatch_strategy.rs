use crate::proto::TopicDispatchStrategy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDispatchStrategy {
    pub strategy: String,
    pub retention_period: u64,
    pub segment_size: usize,
    pub storage_type: StorageType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageType {
    InMemory,
    Disk(String), // Path for local disk storage
    S3(String),   // S3 bucket name
}

impl ConfigDispatchStrategy {
    pub fn new(
        strategy: &str,
        retention_period: u64,
        segment_size: usize,
        storage_type: StorageType,
    ) -> Self {
        ConfigDispatchStrategy {
            strategy: strategy.to_string(),
            retention_period,
            segment_size,
            storage_type,
        }
    }
}

impl Default for ConfigDispatchStrategy {
    fn default() -> Self {
        ConfigDispatchStrategy {
            strategy: "non_reliable".to_string(),
            retention_period: 3600,
            segment_size: 50,
            storage_type: StorageType::InMemory,
        }
    }
}

// Implement conversions from ProtoTypeSchema to SchemaType
impl From<TopicDispatchStrategy> for ConfigDispatchStrategy {
    fn from(strategy: TopicDispatchStrategy) -> Self {
        let storage_type = match strategy.storage_backend {
            0 => StorageType::InMemory,
            1 => StorageType::Disk(strategy.storage_path),
            2 => StorageType::S3(strategy.storage_path),
            _ => StorageType::InMemory,
        };

        ConfigDispatchStrategy {
            strategy: strategy.strategy,
            retention_period: strategy.retention_period,
            segment_size: strategy.segment_size as usize,
            storage_type,
        }
    }
}
// Implement conversions from ConfigRetentionStrategy to ProtoTypeSchema
impl From<ConfigDispatchStrategy> for TopicDispatchStrategy {
    fn from(config: ConfigDispatchStrategy) -> Self {
        let (storage_backend, storage_path) = match config.storage_type {
            StorageType::InMemory => (0, String::new()),
            StorageType::Disk(path) => (1, path),
            StorageType::S3(bucket) => (2, bucket),
        };

        TopicDispatchStrategy {
            strategy: config.strategy,
            retention_period: config.retention_period,
            segment_size: config.segment_size as u64,
            storage_backend,
            storage_path,
        }
    }
}
