use anyhow::Result;
use bytes::BytesMut;

use crate::topic::Topic;

// Represents the connected producer
#[derive(Debug, Clone)]
pub(crate) struct Producer {
    producer_id: u64,
    producer_name: String,
    access_mode: ProducerAccessMode,
}

#[derive(Debug, Clone)]
pub(crate) enum ProducerAccessMode {
    // multiple producers can concurrently produce messages to the same topic
    Shared,
    // nly one producer is allowed to produce messages to the topic
    Exclusive,
}

impl Producer {
    pub(crate) fn new(
        topic: Topic,
        producer_id: u64,
        producer_name: String,
        access_mode: ProducerAccessMode,
    ) -> Self {
        Producer {
            producer_id,
            producer_name,
            access_mode,
        }
    }
    // publish message to topic
    pub(crate) async fn publish_message(
        producer_id: u64,
        message_sequence_id: u64,
        message: BytesMut,
    ) -> Result<()> {
        // it performs some checks then topic.publishMessage
        todo!()
    }
}
