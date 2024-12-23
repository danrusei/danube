use anyhow::{anyhow, Result};
use danube_client::{MessageID, StreamMessage};
use std::collections::HashMap;
use std::sync::{atomic::AtomicUsize, Arc, RwLock};
use tracing::trace;

use crate::{
    consumer::Consumer,
    dispatcher::{
        dispatch_reliable_message_multiple_consumers, dispatch_reliable_message_single_consumer,
    },
    topic_storage::{Segment, TopicStore},
};

/// ConsumerDispatch is holding information about consumers and the messages within a segment
/// It is used to dispatch messages to consumers and to track the progress of the consumer
#[derive(Debug)]
pub(crate) struct ConsumerDispatch {
    // dispatcher type is 0 for single consumer and 1 for multiple consumers
    pub(crate) dispatcher_type: usize,
    // list of consumers
    pub(crate) consumers: Vec<Consumer>,
    // active consumer is the consumer that is currently receiving messages
    // it is used only for single consumer subscriptions
    pub(crate) active_consumer: Option<Consumer>,
    // index consumer is the index of the consumer in the consumers list
    // it is used only for multiple consumer subscriptions
    pub(crate) index_consumer: Arc<AtomicUsize>,
    // topic store is the store of segments
    // topic store is used to get the next segment to be sent to the consumer
    pub(crate) topic_store: TopicStore,
    // last acked segment is the last segment that has all messages acknowledged by the consumer
    // it is used to track the progress of the subscription
    pub(crate) last_acked_segment: Arc<RwLock<usize>>,
    // segment holds the messages to be sent to the consumer
    // segment is replaced when the consumer is done with the segment and if there is another available segment
    pub(crate) segment: Option<Arc<RwLock<Segment>>>,
    // Cached segment ID to avoid frequent locks
    pub(crate) current_segment_id: Option<usize>,
    // single message awaiting acknowledgment from the consumer
    pub(crate) pending_ack_message: Option<(u64, MessageID)>,
    // maps MessageID to request_id of segment acknowledged messages
    pub(crate) acked_messages: HashMap<MessageID, u64>,
}

impl ConsumerDispatch {
    pub(crate) fn new(
        dispatcher_type: usize,
        topic_store: TopicStore,
        last_acked_segment: Arc<RwLock<usize>>,
    ) -> Self {
        Self {
            dispatcher_type,
            consumers: Vec::new(),
            active_consumer: None,
            index_consumer: Arc::new(AtomicUsize::new(0)),
            topic_store,
            last_acked_segment,
            segment: None,
            current_segment_id: None,
            pending_ack_message: None,
            acked_messages: HashMap::new(),
        }
    }

    pub(crate) fn add_single_consumer(&mut self, consumer: Consumer) {
        self.consumers.push(consumer.clone());
        if self.active_consumer.is_none() {
            self.active_consumer = Some(consumer.clone());
        }

        trace!(
            "Consumer {} added to single-consumer dispatcher",
            consumer.consumer_name
        );
    }

    pub(crate) fn add_multiple_consumers(&mut self, consumer: Consumer) {
        self.consumers.push(consumer);
    }

    /// Process the current segment and send the messages to the consumer
    pub(crate) async fn process_current_segment(&mut self) -> anyhow::Result<()> {
        if let Some(segment) = self.segment.as_ref() {
            let current_segment_id = self.current_segment_id;

            // Validate the current segment
            let move_to_next_segment = {
                let segment_id = current_segment_id
                    .ok_or_else(|| anyhow!("Segment ID not cached while processing segment"))?;

                self.validate_segment(segment_id, segment).await?
            };

            // If validation indicates we should move to the next segment
            if move_to_next_segment {
                self.move_to_next_segment()?;
                return Ok(());
            }
        } else {
            // No current segment; attempt to move to the next segment
            self.move_to_next_segment()?;
        }

        // Process the next message using the stored segment
        self.process_next_message().await?;

        Ok(())
    }

    /// Validates the current segment. Returns `true` if the segment was invalidated or closed.
    async fn validate_segment(
        &self,
        segment_id: usize,
        segment: &Arc<RwLock<Segment>>,
    ) -> anyhow::Result<bool> {
        // Check if the segment still exists
        if !self.topic_store.contains_segment(segment_id) {
            tracing::trace!(
                "Segment {} no longer exists, moving to next segment",
                segment_id
            );
            return Ok(true);
        }

        // Check if the segment is closed and all messages are acknowledged
        let segment_data = segment
            .read()
            .map_err(|_| anyhow!("Failed to acquire read lock on segment"))?;
        if segment_data.close_time > 0 && self.acked_messages.len() == segment_data.messages.len() {
            return Ok(true);
        }

        Ok(false)
    }

    /// Moves to the next segment in the `TopicStore`.
    fn move_to_next_segment(&mut self) -> anyhow::Result<()> {
        let next_segment = if let Some(current_segment_id) = self.current_segment_id {
            self.topic_store.get_next_segment(current_segment_id)
        } else {
            self.topic_store.get_next_segment(0) // Start from the first segment
        };

        if let Some(next_segment) = next_segment {
            let next_segment_id = next_segment
                .read()
                .map(|s| s.id)
                .map_err(|_| anyhow!("Failed to acquire read lock on next segment"))?;

            // Update the last acknowledged segment
            if let Some(current_segment_id) = self.current_segment_id {
                let mut last_acked = self
                    .last_acked_segment
                    .write()
                    .map_err(|_| anyhow!("Failed to acquire write lock on last_acked_segment"))?;
                *last_acked = current_segment_id;
            }

            self.segment = Some(next_segment);
            self.current_segment_id = Some(next_segment_id);
        } else {
            self.clear_current_segment();
        }

        Ok(())
    }

    /// Clear the current segment and cached segment_id
    fn clear_current_segment(&mut self) {
        self.segment = None;
        self.current_segment_id = None;
    }

    /// Processes the next unacknowledged message in the current segment.
    async fn process_next_message(&mut self) -> anyhow::Result<()> {
        // Only process next message if there's no pending acknowledgment
        if self.pending_ack_message.is_none() {
            if let Some(segment) = &self.segment {
                let next_message = {
                    let segment_data = segment
                        .read()
                        .map_err(|_| anyhow!("Failed to acquire read lock on segment"))?;
                    segment_data
                        .messages
                        .iter()
                        .find(|msg| !self.acked_messages.contains_key(&msg.msg_id))
                        .cloned()
                };

                if let Some(msg) = next_message {
                    self.pending_ack_message = Some((msg.request_id, msg.msg_id.clone()));
                    self.dispatch_message(msg).await?;
                }
            }
        }

        Ok(())
    }

    // Function to handle message dispatching
    async fn dispatch_message(&mut self, msg: StreamMessage) -> anyhow::Result<()> {
        match self.dispatcher_type {
            0 => {
                dispatch_reliable_message_single_consumer(&mut self.active_consumer, msg).await?;
            }
            1 => {
                dispatch_reliable_message_multiple_consumers(
                    &mut self.consumers,
                    self.index_consumer.clone(),
                    msg,
                )
                .await?;
            }
            _ => {
                anyhow::bail!("Invalid dispatcher type: {}", self.dispatcher_type);
            }
        }
        Ok(())
    }

    /// Handle the consumer message acknowledgement
    pub(crate) async fn handle_message_acked(
        &mut self,
        request_id: u64,
        msg_id: MessageID,
    ) -> Result<()> {
        if let Some((pending_request_id, pending_msg_id)) = &self.pending_ack_message {
            dbg!(
                "received ack for msg with request_id {} and msg_id {:?}",
                request_id,
                &msg_id
            );
            dbg!(
                "pending request_id: {:?}, pending_msg_id: {:?}",
                pending_request_id,
                pending_msg_id
            );
            if *pending_request_id == request_id && *pending_msg_id == msg_id {
                self.pending_ack_message = None;
                self.acked_messages.insert(msg_id.clone(), request_id);
                trace!(
                    "Message with request_id {} and msg_id {:?} acknowledged",
                    request_id,
                    msg_id
                );
                return Ok(());
            }
        }
        Err(anyhow!("Invalid or unexpected acknowledgment"))
    }
}
