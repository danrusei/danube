use anyhow::Result;
use dashmap::DashMap;
use std::collections::HashSet;

use crate::consumer::MessageToSend;

use crate::proto::MessageMetadata;

pub trait RetentionStrategy {
    fn store_message(&self, message: MessageToSend) -> Result<()>;
    fn remove_message(&self, sequence_id: u64) -> Result<()>;
    fn is_ready_for_removal(&self, sequence_id: u64) -> bool;
}

pub struct ReliableRetention {
    // Stores the message until acknowledged
    messages: DashMap<u64, MessageToSend>,
    // Tracks acknowledgments by subscription
    ack_status: DashMap<u64, HashSet<String>>,
}

impl ReliableRetention {
    // Acknowledgment is handled by the subscription,
    // but the logic for removing messages resides in the topic's retention strategy.
    pub(crate) async fn acknowledge_message(
        &self,
        sequence_id: u64,
        subscription_name: String,
    ) -> Result<()> {
        if let Some(mut ack_set) = self.ack_status.get_mut(&sequence_id) {
            ack_set.insert(subscription_name);
        }

        // Check if all subscriptions have acknowledged this message
        if self.is_ready_for_removal(sequence_id) {
            self.remove_message(sequence_id)?;
        }

        Ok(())
    }
}

impl RetentionStrategy for ReliableRetention {
    fn store_message(&self, message: MessageToSend) -> Result<()> {
        // Store the message in memory
        self.messages
            .insert(message.metadata.clone().unwrap().sequence_id, message);
        Ok(())
    }

    fn remove_message(&self, sequence_id: u64) -> Result<()> {
        // Remove message if all subscriptions have acknowledged
        if self.is_ready_for_removal(sequence_id) {
            self.messages.remove(&sequence_id);
        }
        Ok(())
    }

    fn is_ready_for_removal(&self, sequence_id: u64) -> bool {
        // Check if all subscriptions have acknowledged the message
        if let Some(ack_set) = self.ack_status.get(&sequence_id) {
            return ack_set.len() == self.subscriptions.len();
        }
        false
    }
}

pub struct NonReliableRetention;

impl RetentionStrategy for NonReliableRetention {
    fn store_message(&self, _message: MessageToSend) -> Result<()> {
        // No need to store the message
        Ok(())
    }

    fn remove_message(&self, _sequence_id: u64) -> Result<()> {
        // Nothing to remove, non-reliable mode discards after dispatch
        Ok(())
    }

    fn is_ready_for_removal(&self, _sequence_id: u64) -> bool {
        true // Always ready for removal
    }
}
