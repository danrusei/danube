use anyhow::{anyhow, Result};
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
    // acked messages are the messages from the segment that have been acknowledged by the consumer
    pub(crate) acked_messages: Vec<bool>,
    // last acked message index is the index of the last message from the segment that has been acknowledged by the consumer
    pub(crate) last_acked_message_index: u64,
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
            acked_messages: Vec::new(),
            last_acked_message_index: 0,
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
    pub(crate) async fn process_current_segment(&mut self) -> Result<(), String> {
        if let Some(segment) = &self.segment {
            let mut move_to_next_segment = false;

            let message = {
                let segment_lock = segment
                    .read()
                    .map_err(|_| "Failed to acquire read lock on segment")?;

                // Check if the current segment is closed and fully acknowledged
                if segment_lock.close_time > 0 && self.acked_messages.iter().all(|&acked| acked) {
                    move_to_next_segment = true;
                    None
                } else {
                    // Find the next unacknowledged message index
                    let next_message_index = if self.acked_messages.is_empty() {
                        self.acked_messages = vec![false; segment_lock.messages.len()];
                        0
                    } else {
                        self.last_acked_message_index + 1
                    };

                    // Check if there are more messages to send and get the message if available
                    if next_message_index < segment_lock.messages.len() as u64
                        && !self.acked_messages[next_message_index as usize]
                    {
                        Some(segment_lock.messages[next_message_index as usize].clone())
                    } else {
                        None
                    }
                }
            }; // RwLockReadGuard is dropped here

            // If the current segment is closed and fully acknowledged, or there are no more messages, move to the next segment
            if move_to_next_segment || message.is_none() {
                let next_segment = self
                    .topic_store
                    .get_next_segment(self.segment.as_ref().unwrap().read().unwrap().id.clone());

                if let Some(next_segment) = next_segment {
                    // The dispatcher mark the segment as acknowledged on the TopicStore
                    {
                        let mut last_acked = self
                            .last_acked_segment
                            .write()
                            .map_err(|_| "Failed to acquire write lock on last_acked_segment")?;
                        *last_acked = segment
                            .read()
                            .map_err(|_| "Failed to acquire read lock on segment")?
                            .id;
                    }

                    // Assign the next segment
                    self.segment = Some(next_segment);
                    self.acked_messages.clear();
                    self.last_acked_message_index = 0;
                } else if move_to_next_segment {
                    // No next segment available; clear the current segment
                    self.segment = None;
                }

                return Ok(());
            }

            // Dispatch message outside the scope of the lock if we got one
            if let Some(msg) = message {
                match self.dispatcher_type {
                    0 => {
                        dispatch_reliable_message_single_consumer(&mut self.active_consumer, msg)
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                    1 => {
                        dispatch_reliable_message_multiple_consumers(
                            &mut self.consumers,
                            self.index_consumer.clone(),
                            msg,
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    }
                    _ => {
                        return Err(format!("Invalid dispatcher type: {}", self.dispatcher_type));
                    }
                }
            }
        } else {
            // If there is no current segment, attempt to fetch the next one
            let next_segment = self
                .topic_store
                .get_next_segment(self.segment.as_ref().unwrap().read().unwrap().id.clone());

            if let Some(next_segment) = next_segment {
                self.segment = Some(next_segment);
                self.acked_messages.clear();
                self.last_acked_message_index = 0;
            }
        }

        Ok(())
    }

    /// Handle the consumer message acknowledgement
    pub(crate) async fn handle_message_acked(&mut self, message_id: u64) -> Result<()> {
        if let Some(segment) = &self.segment {
            let segment_lock = segment.write().unwrap();
            if message_id < segment_lock.messages.len() as u64 {
                self.acked_messages[message_id as usize] = true;
                self.last_acked_message_index = message_id;
                trace!("Message {} acknowledged by consumer", message_id);
                return Ok(());
            }
        }
        Err(anyhow!("Invalid message ID for acknowledgment"))
    }
}
