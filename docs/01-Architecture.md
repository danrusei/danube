# Danube Architecture

## Danube Pub/Sub Messaging

### Only Danube Pub/Sub Messaging is supported for the moment

Designed for decoupling producers and consumers, enabling asynchronous communication between different parts of a system. Suitable for scenarios where low latency is critical and some message loss is acceptable, such as real-time monitoring / notifications, telemetry data, event-driven architectures.

The messages reside only in memory, providing low latency but not guaranteed to survive broker crashes or consumer disconnections.  The producers are allowed to send messages to Topics even if there are no active consumers. If no consumers are found the messages are droped.

Read [Here](./03-PubSub_messaging_vs_Streaming.md) for more detailed design considerations.

![Danube Stream Architecture](pictures/Danube_architecture_non_persistent.png "Danube Stream Architecture")

### Brokers

A cluster consist of one or more Danube Brokers.

The producers connect to the brokers to publish messages and the consumers connect to the brokers to consume the messages.

Messages are dispatched immediatelly to available consumers, for increased performance.

### MetadataStore

Used for cluster-level metadata storage, configuration and coordination. Maintain the metadata store of the Danube Cluster, such as namespace / topic metadata, broker load data and others.

## Danube Stream (not yet supported)

Designed for processing and analyzing large volumes of data in real-time as it is generated.

In Danube Stream the messages are stored durably on disk (across multiple disks for reliability). This ensures message survival even during broker restarts or consumer failures. Ideal for use cases requiring high reliability and message durability, such as financial transactions, order processing, logging critical events, etc.

![Danube Stream Architecture](pictures/Danube_architecture_persistent.png "Danube Stream Architecture")

### _Brokers

A cluster consist of one or more Danube Brokers.

The producers connect to the brokers to publish messages and the consumers connect to the brokers to consume the messages.

Messages are typically dispatched out of a in memory cache for the sake of performance. If the backlog grows too large for the cache, the broker will start reading entries from the distributed storage.

### _MetadataStore

Used for cluster-level metadata storage, configuration and coordination. Maintain the metadata store of the Danube Cluster, such as namespace / topic metadata, broker load data and others.

### Message Store

It provides message delivery guarantee for applications. If a message successfully reaches the Danube broker, it will be delivered to its intended target.

This guarantee requires that non-acknowledged messages are stored durably until they can be delivered to and acknowledged by consumers. This mode of messaging is commonly called persistent messaging.
