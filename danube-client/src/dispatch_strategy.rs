use crate::proto::{ReliableOptions as ProtoReliableOptions, TopicDispatchStrategy};
use serde::{Deserialize, Serialize};

/// Dispatch strategy for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigDispatchStrategy {
    NonReliable,
    Reliable(ReliableOptions),
}

/// Reliable dispatch strategy options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReliableOptions {
    pub segment_size: usize,
    pub storage_type: StorageType,
    pub retention_policy: RetentionPolicy,
    pub retention_period: u64,
}

impl ReliableOptions {
    pub fn new(
        segment_size: usize,
        storage_type: StorageType,
        retention_policy: RetentionPolicy,
        retention_period: u64,
    ) -> Self {
        ReliableOptions {
            segment_size,
            storage_type,
            retention_policy,
            retention_period,
        }
    }
}

/// Retention policy for messages in the topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionPolicy {
    RetainUntilAck,
    RetainUntilExpire,
}

/// Storage type for messages in the topic. Could be in-memory, disk, or S3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageType {
    InMemory,
    Disk(String), // Path for local disk storage
    S3(String),   // S3 bucket name
}

impl Default for ConfigDispatchStrategy {
    fn default() -> Self {
        ConfigDispatchStrategy::NonReliable
    }
}

// Implement conversions from TopicDispatchStrategy to ConfigDispatchStrategy
impl From<TopicDispatchStrategy> for ConfigDispatchStrategy {
    fn from(strategy: TopicDispatchStrategy) -> Self {
        match strategy.strategy {
            0 => ConfigDispatchStrategy::NonReliable,
            1 => {
                if let Some(reliable_opts) = strategy.reliable_options {
                    let storage_type = match reliable_opts.storage_backend {
                        0 => StorageType::InMemory,
                        1 => StorageType::Disk(reliable_opts.storage_path),
                        2 => StorageType::S3(reliable_opts.storage_path),
                        _ => StorageType::InMemory,
                    };

                    let retention_policy = match reliable_opts.retention_policy {
                        0 => RetentionPolicy::RetainUntilAck,
                        1 => RetentionPolicy::RetainUntilExpire,
                        _ => RetentionPolicy::RetainUntilAck,
                    };

                    ConfigDispatchStrategy::Reliable(ReliableOptions {
                        segment_size: reliable_opts.segment_size as usize,
                        storage_type,
                        retention_policy,
                        retention_period: reliable_opts.retention_period,
                    })
                } else {
                    ConfigDispatchStrategy::NonReliable
                }
            }
            _ => ConfigDispatchStrategy::NonReliable,
        }
    }
}

// Implement conversions from ConfigDispatchStrategy to TopicDispatchStrategy
impl From<ConfigDispatchStrategy> for TopicDispatchStrategy {
    fn from(config: ConfigDispatchStrategy) -> Self {
        match config {
            ConfigDispatchStrategy::NonReliable => TopicDispatchStrategy {
                strategy: 0,
                reliable_options: None,
            },
            ConfigDispatchStrategy::Reliable(opts) => {
                let (storage_backend, storage_path) = match opts.storage_type {
                    StorageType::InMemory => (0, String::new()),
                    StorageType::Disk(path) => (1, path),
                    StorageType::S3(bucket) => (2, bucket),
                };

                let retention_policy = match opts.retention_policy {
                    RetentionPolicy::RetainUntilAck => 0,
                    RetentionPolicy::RetainUntilExpire => 1,
                };

                TopicDispatchStrategy {
                    strategy: 1,
                    reliable_options: Some(ProtoReliableOptions {
                        segment_size: opts.segment_size as u64,
                        storage_backend,
                        storage_path,
                        retention_policy,
                        retention_period: opts.retention_period,
                    }),
                }
            }
        }
    }
}
