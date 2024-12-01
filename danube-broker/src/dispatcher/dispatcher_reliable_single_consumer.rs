use anyhow::{anyhow, Result};
use tokio::{
    sync::mpsc,
    time::{self, Duration},
};
use tracing::{trace, warn};

use crate::{
    consumer::{Consumer, MessageToSend},
    dispatcher::DispatcherCommand,
};

/// Reliable dispatcher for single consumer, it sends ordered messages to a single consumer
#[derive(Debug)]
pub(crate) struct DispatcherReliableSingleConsumer {
    control_tx: mpsc::Sender<DispatcherCommand>,
}

impl DispatcherReliableSingleConsumer {
    pub(crate) fn new() -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);

        // Spawn dispatcher task
        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            let mut active_consumer: Option<Consumer> = None;

            // TODO !!!!! create a struct, as the dispatcher should keep track of messages consumed by consumer
            // the subscription should not send messages directly to dispatcher
            // the dispatcher should decide when and how to send message
            //
            // the subscription should keep track of acknowledgements from consumers and share with dispatcher?
            // or these should be sent to dispatcher as well ? so the dispatcher to decide
            // Polling interval for reliable queue
            //
            // The dispatcher should mark the segment as acknowledged on the TopicStore
            // the Subscription should send the next segment to the dispatcher if the dispatcher have no segment to read from
            // basically when a new message comes, the subscriptions checks also the status of the dispatcher !!!!!!
            let mut interval = time::interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    Some(command) = control_rx.recv() => {
                        match command {
                            DispatcherCommand::AddConsumer(consumer) => {
                                if let Err(e) = Self::handle_add_consumer(
                                    &mut consumers,
                                    &mut active_consumer,
                                    consumer,
                                )
                                .await
                                {
                                    warn!("Failed to add consumer: {}", e);
                                }
                            }
                            DispatcherCommand::RemoveConsumer(consumer_id) => {
                                Self::handle_remove_consumer(
                                    &mut consumers,
                                    &mut active_consumer,
                                    consumer_id,
                                )
                                .await;
                            }
                            DispatcherCommand::DisconnectAllConsumers => {
                                Self::handle_disconnect_all(&mut consumers, &mut active_consumer).await;
                            }
                            DispatcherCommand::DispatchMessage(message) => {
                                unreachable!("Reliable Dispatcher should not receive messages, just segments");
                            }
                            DispatcherCommand::DispatchSegment(segment) => {
                                todo!("Dispatch segment");
                            }
                        }
                    }
                    // Poll message queue for reliable delivery
                        _ = interval.tick() => {
                            // TODO! - don't use the segment if it passed the TTL since closed, go to next segment
                            // TODO! - send ordered messages from the segment to the consumer
                            // TODO! - go to next segment if all messages are acknowledged by consumer
                            if let Err(e) = Self::dispatch_reliable_message(&mut active_consumer, &message_queue).await {
                                warn!("Failed to dispatch reliable message: {}", e);
                            }
                        }
                }
            }
        });

        DispatcherReliableSingleConsumer { control_tx }
    }

    /// Dispatch a message to the active consumer
    pub(crate) async fn dispatch_message(&self, message: MessageToSend) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::DispatchMessage(message))
            .await
            .map_err(|err| anyhow!("Failed to dispatch the message {}", err))
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

    /// Handle adding a consumer
    async fn handle_add_consumer(
        consumers: &mut Vec<Consumer>,
        active_consumer: &mut Option<Consumer>,
        consumer: Consumer,
    ) -> Result<()> {
        if consumer.subscription_type == 1 {
            return Err(anyhow!(
                "Shared subscription should use a multi-consumer dispatcher"
            ));
        }

        if consumer.subscription_type == 0 && !consumers.is_empty() {
            warn!(
                "Exclusive subscription cannot be shared: consumer_id {}",
                consumer.consumer_id
            );
            return Err(anyhow!(
                "Exclusive subscription cannot be shared with other consumers"
            ));
        }

        consumers.push(consumer.clone());

        if active_consumer.is_none() {
            *active_consumer = Some(consumer.clone());
        }

        trace!(
            "Consumer {} added to single-consumer dispatcher",
            consumer.consumer_name
        );

        Ok(())
    }

    /// Handle removing a consumer
    async fn handle_remove_consumer(
        consumers: &mut Vec<Consumer>,
        active_consumer: &mut Option<Consumer>,
        consumer_id: u64,
    ) {
        consumers.retain(|c| c.consumer_id != consumer_id);

        if let Some(ref active) = active_consumer {
            if active.consumer_id == consumer_id {
                *active_consumer = None;
            }
        }

        trace!("Consumer {} removed from dispatcher", consumer_id);

        // Re-pick an active consumer if needed
        if active_consumer.is_none() && !consumers.is_empty() {
            for consumer in consumers {
                if consumer.get_status().await {
                    *active_consumer = Some(consumer.clone());
                    break;
                }
            }
        }
    }

    /// Handle disconnecting all consumers
    async fn handle_disconnect_all(
        consumers: &mut Vec<Consumer>,
        active_consumer: &mut Option<Consumer>,
    ) {
        consumers.clear();
        *active_consumer = None;
        trace!("All consumers disconnected from dispatcher");
    }

    /// Dispatch a message to the active consumer
    async fn handle_dispatch_message(
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
}
