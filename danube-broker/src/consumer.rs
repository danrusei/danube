use anyhow::Result;
use metrics::{counter, gauge};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, trace, warn};

use crate::{
    broker_metrics::{CONSUMER_BYTES_OUT_COUNTER, CONSUMER_MSG_OUT_COUNTER, TOPIC_CONSUMERS},
    proto::MessageMetadata,
};

/// Represents a consumer connected and associated with a Subscription.
#[derive(Debug)]
pub(crate) struct Consumer {
    consumer_id: u64,
    consumer_name: String,
    subscription_type: i32,
    topic_name: String,
    rx_broker: mpsc::Receiver<MessageToSend>,
    tx_cons: mpsc::Sender<MessageToSend>,
    // status = true -> consumer OK, status = false -> Close the consumer
    status: Arc<Mutex<bool>>,
}

#[derive(Debug, Clone)]
pub(crate) struct MessageToSend {
    pub(crate) payload: Vec<u8>,
    pub(crate) metadata: Option<MessageMetadata>,
}

impl Consumer {
    pub(crate) fn new(
        consumer_id: u64,
        consumer_name: &str,
        subscription_type: i32,
        topic_name: &str,
        rx_broker: mpsc::Receiver<MessageToSend>,
        tx_cons: mpsc::Sender<MessageToSend>,
        status: Arc<Mutex<bool>>,
    ) -> Self {
        Consumer {
            consumer_id: consumer_id.into(),
            consumer_name: consumer_name.into(),
            subscription_type,
            topic_name: topic_name.into(),
            rx_broker,
            tx_cons,
            status,
        }
    }

    // The consumer task runs asynchronously, handling message delivery to the gRPC `ReceiverStream`.
    pub(crate) async fn run(&mut self) {
        while let Some(messages) = self.rx_broker.recv().await {
            // Since u8 is exactly 1 byte, the size in bytes will be equal to the number of elements in the vector.
            let payload_size = messages.payload.len();
            // Send the message to the other channel
            if let Err(err) = self.tx_cons.send(messages).await {
                // Log the error and handle the channel closure scenario
                warn!(
                    "Failed to send message to consumer with id: {}. Error: {:?}",
                    self.consumer_id, err
                );

                *self.status.lock().await = false
            } else {
                trace!("Sending the message over channel to {}", self.consumer_id);
                counter!(CONSUMER_MSG_OUT_COUNTER.name, "topic"=> self.topic_name.clone() , "consumer" => self.consumer_id.to_string()).increment(1);
                counter!(CONSUMER_BYTES_OUT_COUNTER.name, "topic"=> self.topic_name.clone() , "consumer" => self.consumer_id.to_string()).increment(payload_size as u64);
            }
        }
        info!("Consumer task ended for consumer_id: {}", self.consumer_id);
    }

    // pub(crate) async fn get_status(&self) -> bool {
    //     *self.status.lock().await
    // }

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

    // closes the consumer from server-side and inform the client through health_check mechanism
    // to disconnect consumer
    // pub(crate) async fn disconnect(&mut self) -> u64 {
    //     gauge!(TOPIC_CONSUMERS.name, "topic" => self.topic_name.to_string()).decrement(1);
    //     *self.status.lock().await = false;
    //     self.consumer_id
    // }
}
