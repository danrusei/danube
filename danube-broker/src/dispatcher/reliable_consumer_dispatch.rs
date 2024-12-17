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

    pub(crate) async fn process_current_segment(&mut self) -> anyhow::Result<()> {
        if let Some(segment) = &self.segment {
            // Use the cached segment_id to verify existence in TopicStore
            let segment_id = self
                .current_segment_id
                .ok_or_else(|| anyhow!("Segment ID not cached while processing segment"))?;

            // Check if the segment still exists, the opened segment always exists in the TopicStore
            // if the current segment does not exist, it means that it has been closed and removed from the TopicStore
            if !self.topic_store.contains_segment(segment_id) {
                tracing::trace!(
                    "Segment {} no longer exists, moving to next segment",
                    segment_id
                );
                self.clear_current_segment(); // Clear segment and cached ID
                self.move_to_next_segment()?; // Fetch the next segment
                return Ok(());
            }

            // Process messages within the current segment
            let mut move_to_next_segment = false;

            if self.pending_ack_message.is_none() {
                let message = {
                    let segment_lock = segment
                        .read()
                        .map_err(|_| anyhow!("Failed to acquire read lock on segment"))?;

                    // Check if segment is closed and all messages are acknowledged
                    if segment_lock.close_time > 0
                        && self.acked_messages.len() == segment_lock.messages.len()
                    {
                        move_to_next_segment = true;
                        None
                    } else {
                        // Find the next unacknowledged message
                        segment_lock
                            .messages
                            .iter()
                            .find(|msg| !self.acked_messages.contains_key(&msg.msg_id))
                            .cloned()
                    }
                };

                if let Some(msg) = message {
                    self.pending_ack_message = Some((msg.request_id, msg.msg_id.clone()));
                    self.dispatch_message(msg).await?;
                } else {
                    move_to_next_segment = true;
                }
            }

            if move_to_next_segment {
                self.move_to_next_segment()?;
            }
        } else {
            self.move_to_next_segment()?;
        }

        Ok(())
    }

    /// Clear the current segment and cached segment_id
    fn clear_current_segment(&mut self) {
        self.segment = None;
        self.current_segment_id = None;
    }

    // Function to handle segment progress and moving to the next segment
    fn move_to_next_segment(&mut self) -> Result<()> {
        if self.segment.is_some() {
            let next_segment = self
                .topic_store
                .get_next_segment(self.current_segment_id.unwrap());

            if let Some(next_segment) = next_segment {
                // Update the last acknowledged segment
                {
                    let mut last_acked = self.last_acked_segment.write().map_err(|_| {
                        anyhow!("Failed to acquire write lock on last_acked_segment")
                    })?;
                    *last_acked = self.current_segment_id.unwrap();
                }
                let next_segment_id = next_segment
                    .read()
                    .map(|s| s.id)
                    .map_err(|_| anyhow!("Failed to acquire read lock on next segment"))?;

                self.segment = Some(next_segment);
                self.current_segment_id = Some(next_segment_id);
            } else {
                self.clear_current_segment();
            }
        } else {
            // If no segment, attempt to fetch a new one
            let next_segment = self.topic_store.get_next_segment(0);
            if let Some(next_segment) = next_segment {
                let segment_id = next_segment
                    .read()
                    .map(|s| s.id)
                    .map_err(|_| anyhow!("Failed to acquire read lock on next segment"))?;

                self.segment = Some(next_segment);
                self.current_segment_id = Some(segment_id);
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
