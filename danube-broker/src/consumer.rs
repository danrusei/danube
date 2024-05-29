use anyhow::{anyhow, Result};
use bytes::Bytes;
use tokio::time::error::Elapsed;
use tracing::trace;

use crate::proto::consumer_request::SubscriptionType;
use crate::subscription::{self, Subscription};

/// Represents a consumer connected and associated with a Subscription.
#[derive(Debug)]
pub(crate) struct Consumer {
    pub(crate) topic_name: String,
    pub(crate) consumer_id: u64,
    pub(crate) consumer_name: String,
    pub(crate) subscription_name: String,
    pub(crate) subscription_type: i32, // should be SubscriptionType,
    pub(crate) tx: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
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
            tx: None,
        }
    }

    // Dispatch a list of entries to the consumer.
    pub(crate) async fn send_messages(&self, messages: Vec<u8>, batch_size: usize) -> Result<()> {
        let unacked_messages = messages.len();
        //Todo! here implement a logic to permit messages if the pendingAcks is under a threshold

        // It attempts to send the message through the tx channel.
        // If sending fails (e.g., if the client disconnects), it breaks the loop.
        if let Some(tx) = &self.tx {
            tx.send(messages).await?;
            trace!("Consumer instace is sending the message over channel");
        } else {
            return Err(anyhow!(
                "unable to send the message, as the tx is not found"
            ));
        };

        Ok(())
    }

    pub(crate) fn set_tx(&mut self, tx: tokio::sync::mpsc::Sender<Vec<u8>>) -> () {
        self.tx = Some(tx);
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
