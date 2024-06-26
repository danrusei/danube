use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

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
    pub(crate) async fn send_messages(&self, messages: Vec<u8>) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.send_messages(messages).await?),
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.send_messages(messages).await?)
            }
        }
    }
    pub(crate) async fn add_consumer(&mut self, consumer: Arc<Mutex<Consumer>>) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.add_consumer(consumer).await?),
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.add_consumer(consumer).await?)
            }
        }
    }
    #[allow(dead_code)]
    pub(crate) async fn remove_consumer(&mut self, consumer: Consumer) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.remove_consumer(consumer).await?),
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.remove_consumer(consumer).await?)
            }
        }
    }
    #[allow(dead_code)]
    pub(crate) async fn get_consumers(&self) -> Option<&Vec<Arc<Mutex<Consumer>>>> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => dispatcher.get_consumers().await,
            Dispatcher::MultipleConsumers(dispatcher) => dispatcher.get_consumers().await,
        }
    }
    // pub(crate) fn additional_method(&self) {
    //     if let Dispatcher::OneConsumer(dispatcher) = self {
    //         dispatcher.additional_method_single();
    //     }
    // }
}
