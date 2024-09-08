use anyhow::Result;
use dashmap::DashMap;
use std::collections::HashSet;

use crate::consumer::MessageToSend;
use crate::retention_strategy::RetentionStrategy;

// Reliable retention strategy
#[derive(Debug, Clone)]
pub struct ReliableRetention {
    messages: DashMap<u64, MessageToSend>,
    ack_status: DashMap<u64, HashSet<String>>,
    subscriptions: HashSet<String>, // Add this field for tracking subscriptions
}

impl ReliableRetention {
    pub(crate) fn new() -> Self {
        ReliableRetention {
            messages: DashMap::new(),
            ack_status: DashMap::new(),
            subscriptions: HashSet::new(),
        }
    }
    pub(crate) async fn acknowledge_message(
        &self,
        sequence_id: u64,
        subscription_name: String,
    ) -> Result<()> {
        if let Some(mut ack_set) = self.ack_status.get_mut(&sequence_id) {
            ack_set.insert(subscription_name);
        }

        if self.is_ready_for_removal(sequence_id) {
            self.remove_message(sequence_id)?;
        }

        Ok(())
    }
}

impl RetentionStrategy for ReliableRetention {
    fn store_message(&self, message: MessageToSend) -> Result<()> {
        self.messages
            .insert(message.metadata.clone().unwrap().sequence_id, message);
        Ok(())
    }

    fn remove_message(&self, sequence_id: u64) -> Result<()> {
        if self.is_ready_for_removal(sequence_id) {
            self.messages.remove(&sequence_id);
        }
        Ok(())
    }

    fn is_ready_for_removal(&self, sequence_id: u64) -> bool {
        if let Some(ack_set) = self.ack_status.get(&sequence_id) {
            return ack_set.len() == self.subscriptions.len();
        }
        false
    }
}
