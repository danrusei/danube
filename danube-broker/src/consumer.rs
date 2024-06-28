use anyhow::Result;
use tracing::{trace, warn};

/// Represents a consumer connected and associated with a Subscription.
#[derive(Debug)]
#[allow(dead_code)]
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
    pub(crate) async fn send_messages(&self, messages: Vec<u8>, _batch_size: usize) -> Result<()> {
        let _unacked_messages = messages.len();
        //Todo! here implement a logic to permit messages if the pendingAcks is under a threshold

        // It attempts to send the message through the tx channel.
        // If sending fails (e.g., if the client disconnects), it breaks the loop.
        if let Some(tx) = &self.tx {
            tx.send(messages).await?;
            trace!("Consumer instace is sending the message over channel");
        } else {
            warn!(
                "unable to send the message to consumer: {} with id: {}, as the tx is not found",
                self.consumer_name, self.consumer_id
            );
        };

        Ok(())
    }

    pub(crate) fn set_tx(&mut self, tx: tokio::sync::mpsc::Sender<Vec<u8>>) -> () {
        self.tx = Some(tx);
    }

    // Close the consumer if: a. the connection is dropped
    // b. all messages were delivered and there are no pending message acks, graceful close connection
    #[allow(dead_code)]
    pub(crate) fn close(&self) -> Result<()> {
        // subscription.remove_consumer(self)
        todo!()
    }

    // Unsubscribe consumer from the Subscription
    #[allow(dead_code)]
    pub(crate) fn unsubscribe(&self) -> Result<()> {
        // subscription.unsubscribe(self)
        todo!()
    }

    // acked message from client
    #[allow(dead_code)]
    pub(crate) fn message_acked(&self) -> Result<()> {
        todo!()
    }

    pub(crate) fn disconnect(&self) -> Result<()> {
        // close the consumer connection
        todo!()
    }
}
