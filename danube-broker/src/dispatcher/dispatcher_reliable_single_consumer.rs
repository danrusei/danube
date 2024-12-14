use anyhow::{anyhow, Result};
use danube_client::StreamMessage;
use std::sync::{Arc, RwLock};
use tokio::{
    sync::mpsc,
    time::{self, Duration},
};
use tracing::{trace, warn};

use crate::{
    consumer::Consumer, dispatcher::ConsumerDispatch, dispatcher::DispatcherCommand,
    topic_storage::TopicStore,
};

/// Reliable dispatcher for single consumer, it sends ordered messages to a single consumer
#[derive(Debug)]
pub(crate) struct DispatcherReliableSingleConsumer {
    control_tx: mpsc::Sender<DispatcherCommand>,
}

impl DispatcherReliableSingleConsumer {
    pub(crate) fn new(topic_store: TopicStore, last_acked_segment: Arc<RwLock<usize>>) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);

        // Spawn dispatcher task
        tokio::spawn(async move {
            let mut consumer_dispatch = ConsumerDispatch::new(0, topic_store, last_acked_segment);
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

        DispatcherReliableSingleConsumer { control_tx }
    }

    /// Add a consumer
    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::AddConsumer(consumer))
            .await
            .map_err(|_| anyhow!("Failed to send add consumer command"))
    }

    /// Remove a consumer
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

/// Handle adding a consumer
async fn handle_add_consumer(
    consumer_dispatch: &mut ConsumerDispatch,
    consumer: Consumer,
) -> Result<()> {
    if consumer.subscription_type == 1 {
        return Err(anyhow!(
            "Shared subscription should use a multi-consumer dispatcher"
        ));
    }

    if consumer.subscription_type == 0 && !consumer_dispatch.consumers.is_empty() {
        warn!(
            "Exclusive subscription cannot be shared: consumer_id {}",
            consumer.consumer_id
        );
        return Err(anyhow!(
            "Exclusive subscription cannot be shared with other consumers"
        ));
    }

    consumer_dispatch.add_single_consumer(consumer);
    trace!("Consumer added to dispatcher");
    Ok(())
}

/// Handle removing a consumer
async fn handle_remove_consumer(consumer_dispatch: &mut ConsumerDispatch, consumer_id: u64) {
    consumer_dispatch
        .consumers
        .retain(|c| c.consumer_id != consumer_id);

    if let Some(active) = &consumer_dispatch.active_consumer {
        if active.consumer_id == consumer_id {
            consumer_dispatch.active_consumer = None;
        }
    }

    if consumer_dispatch.active_consumer.is_none() && !consumer_dispatch.consumers.is_empty() {
        // Use a separate async block to handle the async operation
        for consumer in &consumer_dispatch.consumers {
            if consumer.get_status().await {
                consumer_dispatch.active_consumer = Some(consumer.clone());
                break;
            }
        }
    }

    trace!("Consumer {} removed from dispatcher", consumer_id);
}

/// Handle disconnecting all consumers
async fn handle_disconnect_all(consumer_dispatch: &mut ConsumerDispatch) {
    consumer_dispatch.consumers.clear();
    consumer_dispatch.active_consumer = None;
    trace!("All consumers disconnected from dispatcher");
}

/// Dispatch a message to the active consumer
pub(crate) async fn dispatch_reliable_message_single_consumer(
    active_consumer: &mut Option<Consumer>,
    message: StreamMessage,
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
