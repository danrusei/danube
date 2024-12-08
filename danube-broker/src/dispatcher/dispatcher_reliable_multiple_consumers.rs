use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use tokio::{
    sync::mpsc,
    time::{self, Duration},
};
use tracing::{trace, warn};

use crate::{
    consumer::{Consumer, MessageToSend},
    dispatcher::ConsumerDispatch,
    dispatcher::DispatcherCommand,
    topic_storage::TopicStore,
};

/// Reliable dispatcher for multiple consumers, it sends ordered messages to multiple consumers
#[derive(Debug)]
pub(crate) struct DispatcherReliableMultipleConsumers {
    control_tx: mpsc::Sender<DispatcherCommand>,
}

impl DispatcherReliableMultipleConsumers {
    pub(crate) fn new(topic_store: TopicStore, last_acked_segment: Arc<RwLock<usize>>) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);

        // Spawn dispatcher task
        tokio::spawn(async move {
            let mut consumer_dispatch = ConsumerDispatch::new(1, topic_store, last_acked_segment);
            let mut interval = time::interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    Some(command) = control_rx.recv() => {
                        match command {
                            DispatcherCommand::AddConsumer(consumer) => {
                                if let Err(e) = handle_add_consumer(&mut consumer_dispatch, consumer).await {
                                    warn!("Failed to add consumer: {}", e);
                                }
                            }
                            DispatcherCommand::RemoveConsumer(consumer_id) => {
                                handle_remove_consumer(&mut consumer_dispatch, consumer_id).await;
                            }
                            DispatcherCommand::DisconnectAllConsumers => {
                                handle_disconnect_all(&mut consumer_dispatch).await;
                            }
                            DispatcherCommand::DispatchMessage(_) => {
                                unreachable!("Reliable Dispatcher should not receive messages, just segments");
                            }
                            DispatcherCommand::MessageAcked(message_id) => {
                                if let Err(e) = consumer_dispatch.handle_message_acked(message_id).await {
                                    warn!("Failed to handle message acked: {}", e);
                                }
                            }
                        }
                    }
                    _ = interval.tick() => {
                        // Send ordered messages from the segment to the consumers
                        // Go to the next segment if all messages are acknowledged by consumers
                        // Go to tne next segment if it passed the TTL since closed
                        if let Err(e) = consumer_dispatch.process_current_segment().await {
                            warn!("Failed to process current segment: {}", e);
                        }
                    }
                }
            }
        });

        DispatcherReliableMultipleConsumers { control_tx }
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
}

// Handle adding a consumer
async fn handle_add_consumer(
    consumer_dispatch: &mut ConsumerDispatch,
    consumer: Consumer,
) -> Result<()> {
    consumer_dispatch.add_multiple_consumers(consumer);
    trace!("Consumer added to dispatcher");
    Ok(())
}

/// Handle removing a consumer
async fn handle_remove_consumer(consumer_dispatch: &mut ConsumerDispatch, consumer_id: u64) {
    consumer_dispatch
        .consumers
        .retain(|c| c.consumer_id != consumer_id);

    trace!("Consumer {} removed from dispatcher", consumer_id);
}

/// Handle disconnecting all consumers
async fn handle_disconnect_all(consumer_dispatch: &mut ConsumerDispatch) {
    consumer_dispatch.consumers.clear();
    trace!("All consumers disconnected from dispatcher");
}

/// Dispatch a message to the active consumer
pub(crate) async fn dispatch_reliable_message_single_consumer(
    active_consumer: &mut Option<Consumer>,
    message: MessageToSend,
) -> Result<()> {
    if let Some(consumer) = active_consumer {
        if consumer.get_status().await {
            consumer.send_message(message).await?;
            trace!(
                "Message dispatched to active consumer {}",
                consumer.consumer_id
            );
            return Ok(());
        }
    }

    Err(anyhow!("No active consumer available to dispatch message"))
}

pub(crate) async fn dispatch_reliable_message_multiple_consumers(
    consumers: &mut [Consumer],
    index_consumer: Arc<AtomicUsize>,
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
