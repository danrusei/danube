use anyhow::Result;

use crate::consumer::Consumer;

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
    pub(crate) async fn run(&mut self) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.run().await?),
            Dispatcher::MultipleConsumers(dispatcher) => Ok(dispatcher.run().await?),
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

    pub(crate) async fn disconnect_all_consumers(&mut self) -> Result<Vec<u64>> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
            Dispatcher::MultipleConsumers(dispatcher) => {
                Ok(dispatcher.disconnect_all_consumers().await?)
            }
        }
    }

    pub(crate) fn get_consumers(&self) -> &Vec<Consumer> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => dispatcher.get_consumers(),
            Dispatcher::MultipleConsumers(dispatcher) => dispatcher.get_consumers(),
        }
    }
}
