use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::consumer::{Consumer, MessageToSend};

pub(crate) mod dispatcher_multiple_consumers;
pub(crate) mod dispatcher_single_consumer;
pub(crate) use dispatcher_multiple_consumers::DispatcherMultipleConsumers;
pub(crate) use dispatcher_single_consumer::DispatcherSingleConsumer;

// The dispatchers ensure that messages are routed to consumers according to the semantics of the subscription type
#[derive(Debug)]
pub(crate) enum Dispatcher {
    OneConsumer(DispatcherSingleConsumer),
    MultipleConsumers(DispatcherMultipleConsumers),
}

impl Dispatcher {
    pub(crate) async fn send_messages(&self, messages: MessageToSend) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.send_messages(messages).await?),
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.send_messages(messages).await?)
            }
        }
    }
    pub(crate) async fn disconnect_all_consumers(&self) -> Result<Vec<u64>> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
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
                Ok(dispatcher.remove_consumer(consumer.consumer_id).await?)
            }
        }
    }

    pub(crate) fn get_consumers(&self) -> &Vec<Arc<Mutex<Consumer>>> {
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
