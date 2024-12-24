use crate::proto::TopicDeliveryStrategy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDispatchStrategy {
    pub strategy: String,
    pub retention_period: u64,
    pub segment_size: usize,
}

impl ConfigDispatchStrategy {
    pub fn new(strategy: &str, retention_period: u64, segment_size: usize) -> Self {
        ConfigDispatchStrategy {
            strategy: strategy.to_string(),
            retention_period,
            segment_size,
        }
    }
}

impl Default for ConfigDispatchStrategy {
    fn default() -> Self {
        ConfigDispatchStrategy {
            strategy: "non_reliable".to_string(),
            retention_period: 3600,
            segment_size: 50,
        }
    }
}

// Implement conversions from ProtoTypeSchema to SchemaType
impl From<TopicDeliveryStrategy> for ConfigDispatchStrategy {
    fn from(strategy: TopicDeliveryStrategy) -> Self {
        ConfigDispatchStrategy {
            strategy: strategy.strategy,
            retention_period: strategy.retention_period,
            segment_size: strategy.segment_size as usize,
        }
    }
}
// Implement conversions from ConfigRetentionStrategy to ProtoTypeSchema
impl From<ConfigDispatchStrategy> for TopicDeliveryStrategy {
    fn from(strategy: ConfigDispatchStrategy) -> Self {
        TopicDeliveryStrategy {
            strategy: strategy.strategy,
            retention_period: strategy.retention_period,
            segment_size: strategy.segment_size as u64,
        }
    }
}
