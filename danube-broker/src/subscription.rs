use anyhow::Result;

use crate::consumer::Consumer;

#[derive(Debug, Default)]
pub(crate) struct Subscription {}

#[derive(Debug, Clone)]
pub(crate) struct SubscriptionOption {
    subscription_name: String,
    consumer_id: f32,
    consumer_name: String,
    schema_type: String, // has to be SchemaType as type
}

impl Subscription {
    // add a consumer to the subscription
    // checks if there'a a dispatcher (responsible for distributing messages to consumers)
    // If not initializes a dispatcher based on the type of consumer: Exclusive, Shared, Failover, Key_Shared
    pub(crate) async fn add_consumer(consumer: Consumer) -> Result<()> {
        todo!()
    }

    // remove a consumer from the subscription
    pub(crate) async fn remove_consumer(consumer: Consumer) -> Result<()> {
        // removes consumer from the dispatcher
        //If there are no consumers left after removing the specified one,
        // it unsubscribes the subscription from the topic.
        todo!()
    }

    // handles the disconnection of consumers associated with the subscription.
    pub(crate) async fn disconnect() -> Result<()> {
        //  It checks if there is a dispatcher associated with the subscription.
        // If there is, it calls the disconnectAllConsumers method of the dispatcher
        todo!()
    }

    // Deletes the subscription after it is unsubscribed from the topic and disconnected from consumers.
    pub(crate) async fn delete() -> Result<()> {
        //  It checks if there is a dispatcher associated with the subscription.
        // If there is, it calls the disconnectAllConsumers method of the dispatcher
        todo!()
    }

    // Get Consumers
    pub(crate) async fn get_consumers() -> Result<Vec<Consumer>> {
        // ask dispatcher
        todo!()
    }
}
