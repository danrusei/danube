use anyhow::Result;
use bytes::Bytes;

use crate::subscription::{self, Subscription};

/// Represents a consumer connected and associated with a Subscription.
#[derive(Debug, Default)]
pub(crate) struct Consumer {
    consumer_id: u64,
    consumer_name: String,
}

impl Consumer {
    pub(crate) fn new(
        subscription: Subscription,
        topic_name: String,
        consumer_name: String,
        consumer_id: u64,
    ) -> Self {
        Consumer {
            consumer_id,
            consumer_name,
        }
    }

    // Dispatch a list of entries to the consumer.
    pub(crate) async fn send_messages(entries: Vec<Bytes>, batch_size: u32) -> Result<()> {
        let unacked_messages = entries.len();
        todo!()
    }

    // Close the consumer if: a. the connection is dropped
    // b. all messages were delivered and there are no pending message acks, graceful close connection
    pub(crate) fn close(&self) -> Result<()> {
        // subscription.remove_consumer(self)
        todo!()
    }

    // Unsubscribe consumer from the Subscription
    pub(crate) fn unsubscribe(&self) -> Result<()> {
        // subscription.unsubscribe(self)
        todo!()
    }

    // acked message from client
    pub(crate) fn message_acked() -> Result<()> {
        todo!()
    }
}
