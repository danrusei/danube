use anyhow::{anyhow, Result};
use danube_client::StreamMessage;
use danube_reliable_delivery::ReliableDelivery;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

use crate::proto::TopicDeliveryStrategy;
use crate::topic_storage::TopicStore;

#[derive(Debug)]
pub(crate) enum DeliveryStrategy {
    // Does not store messages, sends them directly to the dispatcher
    NonReliable,
    // Stores messages in a queue for reliable delivery
    // TODO! - ensure that the messages are delivered in order and are acknowledged before removal from the queue
    // TODO! - TTL - implement a retention policy to remove messages from the queue after a certain period of time (e.g. 1 hour)
    Reliable(ReliableDelivery),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ConfigDeliveryStrategy {
    pub(crate) strategy: String,
    pub(crate) retention_period: u64,
    pub(crate) segment_size: usize,
}

impl Default for ConfigDeliveryStrategy {
    fn default() -> Self {
        ConfigDeliveryStrategy {
            strategy: "non_reliable".to_string(),
            retention_period: 3600,
            segment_size: 50,
        }
    }
}

// Implement conversions from ProtoTypeSchema to SchemaType
impl From<TopicDeliveryStrategy> for ConfigDeliveryStrategy {
    fn from(strategy: TopicDeliveryStrategy) -> Self {
        ConfigDeliveryStrategy {
            strategy: strategy.strategy,
            retention_period: strategy.retention_period,
            segment_size: strategy.segment_size as usize,
        }
    }
}
