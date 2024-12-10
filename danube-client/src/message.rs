pub(crate) mod consumer_message;
pub(crate) mod producer_message;

// TODO! messageID is very important as it will be used to identify the message
// it should be constructed by producer, amended maybe by the broker and sent back to the consumer
// the consumer will used the messageID in the ack mechanism so the Broker will easily identify the acked message
// the message id will be transmited over wire as bytes and the below struct will be used by both client SDK and broker
// to identify the message. So the protobuffer message should be modified as well !
// so here we need to add information about the partitions, producer_id sequence_id and any other information that
// that makes easy for both client SDK and broker to identify the message
pub struct MessageId {
    partitionIndex: usize,
}
