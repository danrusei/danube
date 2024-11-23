use anyhow::Result;
use tokio::sync::mpsc;

use crate::consumer::{Consumer, MessageToSend};

pub(crate) mod dispatcher_multiple_consumers;
pub(crate) mod dispatcher_single_consumer;
pub(crate) use dispatcher_multiple_consumers::DispatcherMultipleConsumers;
pub(crate) use dispatcher_single_consumer::DispatcherSingleConsumer;

#[derive(Debug)]
pub(crate) struct DispatcherInfo {
    pub(crate) dispatcher_handle: tokio::task::JoinHandle<()>,
    pub(crate) dispatcher_tx: mpsc::Sender<DispatcherCommand>,
}

#[derive(Debug)]
pub(crate) enum DispatcherCommand {
    AddConsumer(Consumer),
    RemoveConsumer(u64),
    Dispatch(MessageToSend),
    Shutdown,
}

// The dispatchers ensure that messages are routed to consumers according to the semantics of the subscription type
#[derive(Debug)]
pub(crate) enum Dispatcher {
    OneConsumer(DispatcherSingleConsumer),
    MultipleConsumers(DispatcherMultipleConsumers),
}

impl Dispatcher {
    pub(crate) async fn run(&self) -> Result<()> {
        match self {
            Dispatcher::OneConsumer(dispatcher) => Ok(dispatcher.run().await?),
            Dispatcher::MultipleConsumers(dispatcher) => Ok(dispatcher.run().await?),
        }
    }
}
