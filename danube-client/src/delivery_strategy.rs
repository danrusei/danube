use crate::proto::TopicDeliveryStrategy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDeliveryStrategy {
    pub strategy: String,
    pub retention_period: u64,
    pub segment_size: usize,
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
// Implement conversions from ConfigRetentionStrategy to ProtoTypeSchema
impl From<ConfigDeliveryStrategy> for TopicDeliveryStrategy {
    fn from(strategy: ConfigDeliveryStrategy) -> Self {
        TopicDeliveryStrategy {
            strategy: strategy.strategy,
            retention_period: strategy.retention_period,
            segment_size: strategy.segment_size as u64,
        }
    }
}
