# Danube Cluster Services Role

This document enumerates the principal components of the Danube Cluster and their responsibilities.

## Danube Service Components

### Leader Election Service

The Leader Election Service selects one broker from the cluster to act as the Leader. The Broker Leader is responsible for making decisions. This service is used by the Load Manager, ensuring only one broker in the cluster posts the cluster aggregated Load Report.

Leader Election Flow:

* The first broker registered in the cluster becomes the Leader by registering itself on "/cluster/leader".
* The field is registered with a lease, so the leader broker must periodically renew its lease to maintain leadership.
* Subsequent brokers attempt to become leaders but become Followers if the path is already in use.
* All brokers periodically check the leader path. If there is no change, the state is maintained; otherwise, brokers attempt to become the leader.

### Load Manager Service

The Load Manager monitors and distributes load across brokers by managing topic and partition assignments. It implements rebalancing logic to redistribute topics/partitions when brokers join or leave the cluster and is responsible for failover mechanisms to handle broker failures.

Load Manager Flow:

* All brokers periodically post their Load Reports on the path "/cluster/brokers/load/{broker-id}".
* The leader broker watches for load reports from all brokers in the cluster.
* It calculates rankings using the selected Load Balance algorithm.
* It posts its calculations for the cluster on the "/cluster/load_balance" path.

Creation of a New Topic:

* A broker registers the Topic on the "/cluster/unassigned" path.
* The Load Manager of the leader Broker watches this path and assigns the broker with the least load to host the new topic by posting the topic to the "/cluster/brokers/{broker-id}/{topic_name}" path.
* Each broker watches its own path: "/cluster/brokers/{broker-id}". For any event on that path, such as the addition or deletion of topics, it acts accordingly by creating a new topic locally or deleting the topic it owned and all related resources.
* On topic creation, the broker checks if the topic already exists locally. If not, it retrieves all data about the topic, including subscriptions and producers, from the Local Metadata Cache.
* On topic removal, the broker handles the disconnections of producers and consumers and removes the locally allocated resources.

For further consideration: We may want the broker to ask the Load Manager to get the next broker and initiate topic creation. Either it just posts the topic on the "/cluster/unassigned" path, or if it is the selected broker, it also creates the topic locally.

### Local Metadata Cache

This cache stores various types of metadata required by Danube brokers, such as topic and namespace data, which are frequently accessed during message production and consumption. This reduces the need for frequent queries to the central metadata store, ETCD.

The [docs/internal_resources.md](docs/internal_resources.md) document describes how the resources are organized in the Metadata Store.

Updates/events are received via ETCD Watch events and/or the metadata event synchronizer.

### Syncronizer

The synchronizer ensures that metadata and configuration settings across different brokers remain consistent. It propagates changes to metadata and configuration settings using client Producers and Consumers.

This is in addition to Metadata Storage watch events, allowing brokers to process metadata updates even if there was a communication glitch or the broker was unavailable for a short period, potentially missing the Store Watch events. The synchronizer allows for dynamic updates to configuration settings without requiring a broker service restart.

### Danube Broker

The Broker owns the topics and manages their lifecycle. It also facilitates the creation of producers, subscriptions, and consumers, ensuring that producers can publish messages to topics and consumers can consume messages from topics.

## External Metadata Storage (ETCD)

This is the Metadata Storage responsible for the persistent storage of metadata and cluster synchronization.
