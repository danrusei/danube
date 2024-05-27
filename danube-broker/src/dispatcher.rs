use anyhow::Result;
use bytes::Bytes;

use crate::consumer::Consumer;

pub(crate) mod dispatcher_multiple_consumers;
pub(crate) mod dispatcher_single_consumer;
pub(crate) use dispatcher_multiple_consumers::DispatcherMultipleConsumers;
pub(crate) use dispatcher_single_consumer::DispatcherSingleConsumer;

#[derive(Debug)]
pub(crate) enum Dispatcher {
    OneConsumer(DispatcherSingleConsumer),
    MultipleConsumers(DispatcherMultipleConsumers),
}

impl Dispatcher {
    pub(crate) async fn send_messages(&self, messages: Vec<Bytes>) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.send_messages(messages).await?),
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.send_messages(messages).await?)
            }
        }
    }
    pub(crate) fn add_consumer(&mut self, consumer: Consumer) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.add_consumer(consumer)?),
            Dispatcher::MultipleConsumers(dispatcher) => Ok(dispatcher.add_consumer(consumer)?),
        }
    }
    pub(crate) fn remove_consumer(&mut self, consumer: Consumer) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.remove_consumer(consumer)?),
            Dispatcher::MultipleConsumers(dispatcher) => Ok(dispatcher.remove_consumer(consumer)?),
        }
    }
    pub(crate) fn get_consumers(&self) -> Option<&Vec<Consumer>> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => dispatcher.get_consumers(),
            Dispatcher::MultipleConsumers(dispatcher) => dispatcher.get_consumers(),
        }
    }
    // pub(crate) fn additional_method(&self) {
    //     if let Dispatcher::OneConsumer(dispatcher) = self {
    //         dispatcher.additional_method_single();
    //     }
    // }
}
