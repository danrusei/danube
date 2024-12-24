use anyhow::{anyhow, Result};
use danube_client::StreamMessage;
use danube_reliable_dispatch::SubscriptionDispatch;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{
    sync::mpsc,
    time::{self, Duration},
};
use tracing::{trace, warn};

use crate::{consumer::Consumer, dispatcher::DispatcherCommand, message::AckMessage};

/// Reliable dispatcher for multiple consumers, it sends ordered messages to multiple consumers
#[derive(Debug)]
pub(crate) struct DispatcherReliableMultipleConsumers {
    control_tx: mpsc::Sender<DispatcherCommand>,
}

impl DispatcherReliableMultipleConsumers {
    pub(crate) fn new(mut subscription_dispatch: SubscriptionDispatch) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);

        // Spawn dispatcher task
        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            let index_consumer = AtomicUsize::new(0);
            let mut interval = time::interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    Some(command) = control_rx.recv() => {
                        match command {
                            DispatcherCommand::AddConsumer(consumer) => {
                            consumers.push(consumer);
                            trace!("Consumer added. Total consumers: {}", consumers.len());
                        }
                            DispatcherCommand::RemoveConsumer(consumer_id) => {
                            consumers.retain(|c| c.consumer_id != consumer_id);
                            trace!("Consumer removed. Total consumers: {}", consumers.len());
                        }
                             DispatcherCommand::DisconnectAllConsumers => {
                            consumers.clear();
                            trace!("All consumers disconnected.");
                        }
                            DispatcherCommand::DispatchMessage(_) => {
                                unreachable!("Reliable Dispatcher should not receive messages, just segments");
                            }
                            DispatcherCommand::MessageAcked(request_id, msg_id) => {
                                if let Err(e) = subscription_dispatch.handle_message_acked(request_id, msg_id).await {
                                    warn!("Failed to handle message acked: {}", e);
                                }
                            }
                        }
                    }
                    _ = interval.tick() => {
                        // Send ordered messages from the segment to the consumers
                        // Go to the next segment if all messages are acknowledged by consumers
                        // Go to tne next segment if it passed the TTL since closed
                        match subscription_dispatch.process_current_segment().await {
                            Ok(msg) => {
                                if let Err(e) = Self::dispatch_reliable_message_multiple_consumers(
                                    &mut consumers,
                                    &index_consumer,
                                    msg,
                                ).await {
                                    warn!("Failed to dispatch message: {}", e);
                                }
                            },
                            Err(_) => {
                            // as this loops, the error is due to waiting for a new message, so we just ignore it
                            // warn!("Failed to process current segment: {}", e);
                            }
                        };
                    }
                }
            }
        });

        DispatcherReliableMultipleConsumers { control_tx }
    }

    /// Acknowledge a message, which means that the message has been successfully processed by the consumer
    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::MessageAcked(
                ack_msg.request_id,
                ack_msg.msg_id,
            ))
            .await
            .map_err(|_| anyhow!("Failed to send message acked command"))
    }

    /// Add a new consumer to the dispatcher
    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::AddConsumer(consumer))
            .await
            .map_err(|_| anyhow!("Failed to send add consumer command"))
    }

    /// Remove a consumer by its ID
    #[allow(dead_code)]
    pub(crate) async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::RemoveConsumer(consumer_id))
            .await
            .map_err(|_| anyhow!("Failed to send remove consumer command"))
    }

    /// Disconnect all consumers
    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::DisconnectAllConsumers)
            .await
            .map_err(|_| anyhow!("Failed to send disconnect all consumers command"))
    }

    pub(crate) async fn dispatch_reliable_message_multiple_consumers(
        consumers: &mut [Consumer],
        index_consumer: &AtomicUsize,
        message: StreamMessage,
    ) -> Result<()> {
        let num_consumers = consumers.len();
        if num_consumers == 0 {
            return Err(anyhow!("No consumers available to dispatch the message"));
        }

        for _ in 0..num_consumers {
            let index = index_consumer.fetch_add(1, Ordering::SeqCst) % num_consumers;
            let consumer = &mut consumers[index];

            if consumer.get_status().await {
                consumer.send_message(message).await?;
                trace!(
                    "Dispatcher sent the message to consumer: {}",
                    consumer.consumer_id
                );
                return Ok(());
            }
        }

        Err(anyhow!(
            "No active consumers available to handle the message"
        ))
    }
}
