use danube_client::{ConfigDispatchStrategy, ReliableOptions, RetentionPolicy, StorageType};
use danube_reliable_dispatch::ReliableDispatch;

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
