use anyhow::Result;

// Represents the connected producer
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct Producer {
    pub(crate) producer_id: u64,
    pub(crate) producer_name: String,
    pub(crate) topic_name: String,
    pub(crate) access_mode: i32, // should be ProducerAccessMode
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum ProducerAccessMode {
    // multiple producers can concurrently produce messages to the same topic
    Shared,
    // nly one producer is allowed to produce messages to the topic
    Exclusive,
}

impl Producer {
    pub(crate) fn new(
        producer_id: u64,
        producer_name: String,
        topic_name: String,
        access_mode: i32,
    ) -> Self {
        Producer {
            producer_id,
            producer_name,
            topic_name,
            access_mode,
        }
    }
    // publish message to topic
    pub(crate) async fn publish_message(
        &self,
        _producer_id: u64,
        _message_sequence_id: u64,
        _message: &Vec<u8>,
    ) -> Result<()> {
        // it performs some checks in regards to checksum, encryption etc
        // and it calls the topic.publish_message()

        //let's assume that the checks pass and let the broker_server to call the topic.publish_message

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn get_id(&self) -> u64 {
        self.producer_id
    }
}
