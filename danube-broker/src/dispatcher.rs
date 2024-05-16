use crate::{subscription::Subscription, topic::Topic};

#[derive(Debug, Clone)]
pub(crate) enum SubscriptionType {
    // only one consumer is allowed to receive messages from the subscription
    Exclusive,
    // Multiple consumers can connect to the subscription,
    // but only one consumer (the active consumer) receives messages at any given time.
    Failover,
    // multiple consumers can subscribe to the same subscription and receive messages concurrently.
    // messages from the subscription are load-balanced across all connected consumers
    Shared,
    // similar to Shared subscription but with the ability to partition messages based on a message key.
    // messages with the same key are always delivered to the same consumer within a subscription,
    // KeyShared subscriptions are useful for scenarios where message ordering based on a key attribute is required.
    // KeyShared, - not supported yet
}

#[derive(Debug)]
pub(crate) struct DispatcherSingleConsumer {
    subscription_type: SubscriptionType,
    topic: Topic,
    subsc: Subscription,
}

impl DispatcherSingleConsumer {
    pub(crate) fn new(
        subscription_type: SubscriptionType,
        topic: Topic,
        subsc: Subscription,
    ) -> Self {
        DispatcherSingleConsumer {
            subscription_type,
            topic,
            subsc,
        }
    }
}

#[derive(Debug)]
pub(crate) struct DispatcherMultipleConsumers {}
