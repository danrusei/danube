use anyhow::Result;

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

// Control messages for the dispatcher
enum DispatcherCommand {
    AddConsumer(Consumer),
    RemoveConsumer(u64),
    DisconnectAllConsumers,
    DispatchMessage(MessageToSend),
}

impl Dispatcher {
    pub(crate) async fn dispatch_message(&self, message: MessageToSend) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.dispatch_message(message).await?),
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.dispatch_message(message).await?)
            }
        }
    }
    pub(crate) async fn add_consumer(&mut self, consumer: Consumer) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.add_consumer(consumer).await?),
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.add_consumer(consumer).await?)
            }
        }
    }
    #[allow(dead_code)]
    pub(crate) async fn remove_consumer(&mut self, consumer_id: u64) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => {
                Ok(dispatcher.remove_consumer(consumer_id).await?)
            }
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.remove_consumer(consumer_id).await?)
            }
        }
    }

    pub(crate) async fn disconnect_all_consumers(&mut self) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
        }
    }
}
