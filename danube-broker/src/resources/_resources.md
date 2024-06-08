# Resources mapping

This document describes how the resources are organized in the Metadata store

## Resources Types

* **Cluster resources** - holds information about the cluster and the cluster's brokers
* **Namespace resources** - holds information about the namespace policy and the namespace's topics
* **Topic resources** - holds information about the topic policy and topic metadata, including partitioned topics
* **Subscription resources** - holds information about the topic subscriptions, including their consumers
* **Producer resources** - holds information about the producers

### Cluster Resources

* /cluster/cluster-name
* /cluster/brokers/{broker-id}/{namespace}/{topic} - topics served by the broker
* /cluster/brokers/load/{broker-id} - this is where each broker periodically reports its load metrics
* /cluster/load_balance - this is load_balance updated decision, posted by the Load Manager

### Namespace Resources

* /namespace/{namespace}/policy
* /namespace/{namespace}/topics - holds a list with the associated topics

Example Keys:

*/namespace/markets/policy* - where *markets* is the namespace that stores a Json like { "retentionTimeInMinutes": 1440 }

### Topic Resources

* /topic/{namespace}/{topic}  - holds the topic metadata
* /topic/{namespace}/{topic}/policy - holds the topic policy

Example Keys:

*/namespace/markets/trade-events-part-1* - where */markets/trade-events-part-1* is the partitioned topic that stores topic partition metadata

### Subscriptions Resources

* /subscription/{subscription_name} - holds the subscription metadata
* /subscription/{subscription_name}/{consumer_id} - holds the consumer metadata

### Producers Resources

* /producer/{producer-id} - holds the producer metadata
* /producer/{producer-id}/config - holds the producer config
