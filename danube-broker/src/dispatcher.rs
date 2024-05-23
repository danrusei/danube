use bytes::Bytes;

use crate::consumer::Consumer;

pub(crate) mod dispatcher_multiple_consumers;
pub(crate) mod dispatcher_single_consumer;

pub(crate) trait Dispatcher {
    fn send_messages(&self, messages: Vec<Bytes>);
    fn add_consumer(&self, consumer: Consumer);
    fn remove_consumer(&self, consumer: Consumer);
    fn get_consumers(&self) -> Vec<Consumer>;
}
