# Resources mapping

This document describes how the resources are organized in the Metadata store

## MetadataStore and LocalCache

Basically the entire configuration and the metadata for all the cluster's objects (topics, namespaces, etc) are stored in **MetadataStorage (ETCD)** and in the **LocalCache** to ensure fast retrieval for the local broker and to reduce the number of request to the metadata database.

The pattern:

* **Put / Delete** requests should use MetadataStore (ETCD), to ensure consistency across cluster
* **Get** requests should be served from the Local Cache

The **LocalCache** continuously update from 2 sources for increase consistency:

* the Watch operation on ETCD
* the Syncronizer topic, where all **Put / Delete** requests are published and read by the brokers.

## Resources Types

### Cluster Resources

Holds information about the cluster and the cluster's brokers. Mainly read and write by Danube Service.

* **/cluster/cluster-name**
  * holds a String with the name of the cluster
* **/cluster/register/{broker-id}**
  * the broker register once it join the cluster, contain the broker metadata (broker id & socket addr)  
* **/cluster/brokers/{broker-id}/{namespace}/{topic}**
  * topics served by the broker, with value ()
  * **Load Manager** updates the path, with topic assignments to brokers
  * **Brokers** should watch it's own path like (/cluster/brokers/1122334455) - and perform the neccesary actions on adding or removing a topic
* **/cluster/unassigned/{namespace}/{topic}**
  * New unassigned topics created by Broker
  * Load Manager should watch this path, add assign the topic to a broker
* **/cluster/load/{broker-id}**
  * broker periodically reports its load metrics on this path
  * Load Manager watch this path to calculate broker load rankings for the cluster
* **/cluster/load_balance**
  * the load_balance updated decision, posted by the Load Manager, contain a HashMap with keys the broker_id and value the list of topic_name
* **/cluster/leader**
  * the value posted by Leader Election service, it holds broker_id of the current Leader of the CLuster

Example:

* /cluster/brokers/1122334455/markets/trade-events - value is ()
* /cluster/brokers/1122334455/markets/trade-events - value is ()

### Namespace Resources

Holds information about the namespace policy and the namespace's topics

* **/namespaces/{namespace}/policy**
* **/namespaces/{namespace}/topics/{namespace}/{topic}**

Example:

* /namespaces/markets/policy - the value stores a Json like { "retentionTimeInMinutes": 1440 }
* /namespaces/markets/topics/markets/trade-events - topics part of the namespace, value is ()

### Topic Resources

Holds information about the topic policy and the associated producers / subscriptions, including partitioned topic.

* **/topics/{namespace}/{topic}/policy**
  * holds the topic policy, the value stores a Json
* **/topics/{namespace}/{topic}/schema**
  * holds the topic schema, the value stores the schema
* **/topics/{namespace}/{topic}/producers/{producer_id}**
  * holds the producer config
* **/topics/{namespace}/{topic}/subscriptions/{subscription_name}**
  * holds the subscription config

Example:

* /topics/markets/trade-events/producers/1122334455 - with value Producer Metadata
* /topics/markets/trade-events/subscriptions/my_subscription - with value Subscription Metadata
* /topics/markets/trade-events-part-1/policy - where */markets/trade-events-part-1* is the partitioned topic that stores partition policy

### Subscriptions Resources

Holds information about the topic subscriptions, including associated consumers

* **/subscriptions/{subscription_name}/{consumer_id}**
  * holds the consumer metadata

Example:

* /subscriptions/my_subscription/23232323
