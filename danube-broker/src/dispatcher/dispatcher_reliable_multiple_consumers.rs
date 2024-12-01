use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{
    sync::mpsc,
    time::{self, Duration},
};
use tracing::{trace, warn};

use crate::{
    consumer::{Consumer, MessageToSend},
    dispatcher::DispatcherCommand,
};

/// Reliable dispatcher for multiple consumers, it sends ordered messages to multiple consumers
#[derive(Debug)]
pub(crate) struct DispatcherReliableMultipleConsumers {
    control_tx: mpsc::Sender<DispatcherCommand>,
}

impl DispatcherReliableMultipleConsumers {
    pub(crate) fn new() -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);

        // Spawn the dispatcher task
        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            let mut index_consumer = AtomicUsize::new(0);

            // Polling interval for reliable queue
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
                                unreachable!(
                                    "Reliable Dispatcher should not receive messages, just segments"
                                )
                            }
                            DispatcherCommand::DispatchSegment(segment) => {
                                todo!("Dispatching segment: {:?}", &segment);
                            }
                        }
                    }
                     _ = interval.tick() => {
                            // TODO! - don't use the segment if it passed the TTL since closed, go to next segment
                            // TODO! - send ordered messages from the segment to the consumers
                            // TODO! - go to next segment if all messages are acknowledged by consumers
                                if let Err(e) = Self::dispatch_reliable_message(&mut active_consumer, &message_queue).await {
                                    warn!("Failed to dispatch reliable message: {}", e);
                                }
                            }
                }
            }
        });

        DispatcherReliableMultipleConsumers { control_tx }
    }

    /// Dispatch a message to the active consumer
    pub(crate) async fn dispatch_message(&self, message: MessageToSend) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::DispatchMessage(message))
            .await
            .map_err(|err| anyhow!("Failed to dispatch the message {}", err))
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

    /// Dispatch message helper method
    async fn handle_dispatch_message(
        consumers: &mut [Consumer],
        index_consumer: &mut AtomicUsize,
        message: MessageToSend,
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
