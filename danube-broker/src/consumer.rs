use anyhow::Result;
use bytes::Bytes;

use crate::proto::consumer_request::SubscriptionType;
use crate::subscription::{self, Subscription};

/// Represents a consumer connected and associated with a Subscription.
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Consumer {
    pub(crate) topic_name: String,
    pub(crate) consumer_id: u64,
    pub(crate) consumer_name: String,
    pub(crate) subscription_name: String,
    pub(crate) subscription_type: i32, // should be SubscriptionType,
}

impl Consumer {
    pub(crate) fn new(
        topic_name: &str,
        consumer_id: u64,
        consumer_name: &str,
        subscription_name: &str,
        subscription_type: i32, // should be SubscriptionType,
    ) -> Self {
        Consumer {
            topic_name: topic_name.into(),
            consumer_id: consumer_id.into(),
            consumer_name: consumer_name.into(),
            subscription_name: subscription_name.into(),
            subscription_type,
        }
    }

    // Dispatch a list of entries to the consumer.
    pub(crate) async fn send_messages(&self, entries: Vec<Bytes>, batch_size: usize) -> Result<()> {
        let unacked_messages = entries.len();
        //Todo! here implement a logic to permit messages if the pendingAcks is under a threshold
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
    pub(crate) fn message_acked(&self) -> Result<()> {
        todo!()
    }

    pub(crate) fn disconnect(&self) -> Result<()> {
        // close the consumer connection
        todo!()
    }
}
