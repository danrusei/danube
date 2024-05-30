// Different starting paths create a clear hierarchical structure
// that reflects the logical organization of the Danube messaging system.
// It allows for the separation of concerns & efficient querying, ensuring that publisher, consumer, topic,
// and namespace configurations are neatly organized and do not get mixed up.
// Reduced Overhead: By limiting the scope of data retrievals to a specific path,
// the overhead of processing unnecessary data, is reduced.
//
// Namespace:
// /namespace/{namespace}/{policy-name}
// Ex Key: /namespace/markets/retention that stores a Json like { "retentionTimeInMinutes": 1440 }
// Ex Key: /namespace/markets/config that stores the namespace configuration
//
// Topic:
// /topic/{namespace}/{topic}/{policy-name}
// Ex Key: /topic/markets/trade-events/maxConsumers that host an Json like { "maxConsumers": 20 }
// Ex Key: /topic/markets/trade-events/subscriptions that stores the names of all subscriptions
// Ex Key: /topic/markets/trade-events/consig that stores the topic config
//
// Same for Subscription, Publisher & Consumers
// /publisher/{namespace}/{topic}/{publisher-id}/config -> /publisher/markets/trade-events/publisher-123/config
// /consumer/{namespace}/{topic}/{consumer-id}/config -> /consumer/markets/trade-events/consumer-456/config
//
// Resources provides the mechanisms to store and retrieve specific information from MetadataStore
#[derive(Debug, Default)]
pub(crate) struct Resources {
    cluster: ClusterResources,
    namespace: NamespaceResources,
    topic: TopicResources,
    // should hold also the MetadataStore,
    // as the resources translate the Danube requests into MetadataStore paths puts & gets
}

impl Resources {
    pub(crate) fn new() -> Self {
        Resources::default()
    }
}

#[derive(Debug, Default)]
pub(crate) struct ClusterResources {}

#[derive(Debug, Default)]
pub(crate) struct NamespaceResources {}

#[derive(Debug, Default)]
pub(crate) struct TopicResources {}
