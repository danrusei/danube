use crate::proto::{MsgId, StreamMessage as ProtoStreamMessage};
use danube_client::{MessageID, StreamMessage};

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
