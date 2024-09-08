use anyhow::Result;

use crate::consumer::MessageToSend;
use crate::retention_strategy::RetentionStrategy;

// Non-reliable retention strategy
#[derive(Debug, Clone)]
pub struct NonReliableRetention;

impl NonReliableRetention {
    pub(crate) fn new() -> Self {
        NonReliableRetention {}
    }
}

impl RetentionStrategy for NonReliableRetention {
    fn store_message(&self, _message: MessageToSend) -> Result<()> {
        Ok(())
    }

    fn remove_message(&self, _sequence_id: u64) -> Result<()> {
        Ok(())
    }

    fn is_ready_for_removal(&self, _sequence_id: u64) -> bool {
        true
    }
}
