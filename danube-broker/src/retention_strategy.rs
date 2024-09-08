mod non_reliable_retention;
mod reliable_retention;

pub(crate) use non_reliable_retention::NonReliableRetention;
pub(crate) use reliable_retention::ReliableRetention;

use anyhow::Result;
use std::fmt;
use std::str::FromStr;

use crate::consumer::MessageToSend;

// Define the RetentionStrategyType enum
#[derive(Debug, Clone)]
pub enum RetentionStrategyType {
    Reliable(ReliableRetention),
    NonReliable(NonReliableRetention),
}

// Implement ToString for RetentionStrategyType to return the names of the strategies
impl fmt::Display for RetentionStrategyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RetentionStrategyType::Reliable(_) => write!(f, "ReliableRetention"),
            RetentionStrategyType::NonReliable(_) => write!(f, "NonReliableRetention"),
        }
    }
}

// Implement FromStr for RetentionStrategyType
impl FromStr for RetentionStrategyType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ReliableRetention" => Ok(RetentionStrategyType::Reliable(ReliableRetention::new())),
            "NonReliableRetention" => Ok(RetentionStrategyType::NonReliable(
                NonReliableRetention::new(),
            )),
            _ => Err(format!(
                "Invalid retention_type, allowed values: ReliableRetention, NonReliableRetention"
            )),
        }
    }
}

// Trait for retention strategies
pub trait RetentionStrategy {
    fn store_message(&self, message: MessageToSend) -> Result<()>;
    fn remove_message(&self, sequence_id: u64) -> Result<()>;
    fn is_ready_for_removal(&self, sequence_id: u64) -> bool;
}

// Implement RetentionStrategy for RetentionStrategyType enum
impl RetentionStrategy for RetentionStrategyType {
    fn store_message(&self, message: MessageToSend) -> Result<()> {
        match self {
            RetentionStrategyType::Reliable(strategy) => strategy.store_message(message),
            RetentionStrategyType::NonReliable(strategy) => strategy.store_message(message),
        }
    }

    fn remove_message(&self, sequence_id: u64) -> Result<()> {
        match self {
            RetentionStrategyType::Reliable(strategy) => strategy.remove_message(sequence_id),
            RetentionStrategyType::NonReliable(strategy) => strategy.remove_message(sequence_id),
        }
    }

    fn is_ready_for_removal(&self, sequence_id: u64) -> bool {
        match self {
            RetentionStrategyType::Reliable(strategy) => strategy.is_ready_for_removal(sequence_id),
            RetentionStrategyType::NonReliable(strategy) => {
                strategy.is_ready_for_removal(sequence_id)
            }
        }
    }
}
