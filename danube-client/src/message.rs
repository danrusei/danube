use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};

use crate::proto::{MsgId, StreamMessage as ProtoStreamMessage};

// TODO! messageID is very important as it will be used to identify the message
// it should be constructed by producer, amended maybe by the broker and sent back to the consumer
// the consumer will used the messageID in the ack mechanism so the Broker will easily identify the acked message
// the below struct will be used by both client SDK and broker to identify the message.

#[derive(Debug, Clone)]
pub struct MessageID {
    // the messsage sequence id, this is the sequence id of the message within the topic
    pub sequence_id: u64,
    // broker_addr is the address of the broker that sent the message to the consumer
    // this is required by the consumer to send the ack to the correct broker
    pub broker_addr: String,
    // topic_name is the name of the topic the message belongs to
    // this is required by the broker to send the ack to the correct topic
    pub topic_name: String,
    // subscription_name is the name of the subscription the consumer is subscribed to
    // this is required by the broker to send the ack to the correct subscription
    pub subscription_name: String,
}

impl Display for MessageID {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}_{}_{}_{}",
            self.sequence_id, self.broker_addr, self.topic_name, self.subscription_name
        )
    }
}

#[derive(Debug, Clone)]
pub struct StreamMessage {
    // Unique ID for tracking the message request
    pub request_id: u64,
    // Identifies the message, associated with a unique topic, subscription and the broker
    pub msg_id: MessageID,
    // The actual payload of the message
    pub payload: Vec<u8>,
    // Timestamp for when the message was published
    pub publish_time: u64,
    // Identifies the producer’s name
    pub producer_name: String,
    // Identifies the producer, associated with a unique topic
    pub producer_id: u64,
    // User-defined properties/attributes
    pub attributes: HashMap<String, String>,
}

impl From<MsgId> for MessageID {
    fn from(proto_msg_id: MsgId) -> Self {
        MessageID {
            sequence_id: proto_msg_id.sequence_id,
            broker_addr: proto_msg_id.broker_addr,
            topic_name: proto_msg_id.topic_name,
            subscription_name: proto_msg_id.subscription_name,
        }
    }
}

impl From<ProtoStreamMessage> for StreamMessage {
    fn from(proto_stream_msg: ProtoStreamMessage) -> Self {
        StreamMessage {
            request_id: proto_stream_msg.request_id,
            msg_id: proto_stream_msg.msg_id.map_or_else(
                || panic!("Message ID cannot be None"),
                |msg_id| msg_id.into(),
            ),
            payload: proto_stream_msg.payload,
            publish_time: proto_stream_msg.publish_time,
            producer_name: proto_stream_msg.producer_name,
            producer_id: proto_stream_msg.producer_id,
            attributes: proto_stream_msg.attributes,
        }
    }
}

impl From<MessageID> for MsgId {
    fn from(msg_id: MessageID) -> Self {
        MsgId {
            sequence_id: msg_id.sequence_id,
            broker_addr: msg_id.broker_addr,
            topic_name: msg_id.topic_name,
            subscription_name: msg_id.subscription_name,
        }
    }
}

impl From<StreamMessage> for ProtoStreamMessage {
    fn from(stream_msg: StreamMessage) -> Self {
        ProtoStreamMessage {
            request_id: stream_msg.request_id,
            msg_id: Some(stream_msg.msg_id.into()), // Convert MessageID into MsgId
            payload: stream_msg.payload,
            publish_time: stream_msg.publish_time,
            producer_name: stream_msg.producer_name,
            producer_id: stream_msg.producer_id,
            attributes: stream_msg.attributes,
        }
    }
}
