# Resources mapping

This document describes how the resources are organized in the Metadata store

## MetadataStorage and LocalCache

Basically the entire configuration and the metadata for all the cluster's objects (topics, namespaces, etc) are stored in **MetadataStorage (ETCD)** and in the **LocalCache** to ensure fast retrieval for the local broker and to reduce the number of request to the metadata database.

The pattern:

* **Put / Delete** requests should use MetadataStorage (ETCD), to ensure consistency across cluster
* **Get** requests will be served from the Local Cache

The LocalCache continuously update from 2 sources for increase consistency:

* Watch operation on ETCD
* and backed by syncronizer topic, where all **Put / Delete** requests are published and read by the brokers.

## Resources Types

* **Cluster resources** - holds information about the cluster and the cluster's brokers
* **Namespace resources** - holds information about the namespace policy and the namespace's topics
* **Topic resources** - holds information about the topic policy and topic metadata, including partitioned topics
* **Subscription resources** - holds information about the topic subscriptions, including their consumers
* **Producer resources** - holds information about the producers

### Cluster Resources

(read and write only by Danube Service)

* /cluster/cluster-name
* /cluster/brokers/{broker-id}/{namespace}/{topic} - topics served by the broker
* /cluster/brokers/load/{broker-id} - this is where each broker periodically reports its load metrics
* /cluster/load_balance - this is load_balance updated decision, posted by the Load Manager

### Namespace Resources

* /namespace/{namespace}/policy
* /namespace/{namespace}/topics/{namespace}/{topic}

Example Keys:

*/namespace/markets/policy* - where *markets* is the namespace that stores a Json like { "retentionTimeInMinutes": 1440 }
*/namespace/markets/topics/markets/trade-events* - where the last 2 items are the topic_name (markets/trade-events)

### Topic Resources

* /topic/{namespace}/{topic}  - holds the topic metadata
* /topic/{namespace}/{topic}/policy - holds the topic policy

Example Keys:

*/topic/markets/trade-events-part-1* - where */markets/trade-events-part-1* is the partitioned topic that stores topic partition metadata

### Subscriptions Resources

* /subscription/{subscription_name} - holds the subscription metadata
* /subscription/{subscription_name}/{consumer_id} - holds the consumer metadata

### Producers Resources

* /producer/{producer-id} - holds the producer metadata
* /producer/{producer-id}/config - holds the producer config
