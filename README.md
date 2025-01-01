# Danube

Danube is an open-source distributed Pub/Sub messaging platform (inspired by Apache Pulsar).
Danube is designed for high-performance and scalable message queueing, suitable for event-driven applications. It supports both message queueing and fan-out pub-sub systems, making it versatile for various use cases.

Check-out [the Docs](https://dev-state.com/danube_docs/) for more details of the Danube Architecture and the supported concepts.

## Key Capabilities of the Danube Platform

* **Topics**: A unit of storage that organizes messages into a stream.
  * **Non-partitioned topics**: Served by a single broker.
  * **Partitioned topics**: Divided into partitions, served by different brokers within the cluster, enhancing scalability and fault tolerance.
* **Message Dispatch**:
  * **Non-reliable Message Dispatch**: Messages reside in memory and are promptly distributed to consumers, ideal for scenarios where speed is crucial.
  * **Reliable Message Dispatch**: Supports configurable storage options including in-memory, disk, and S3, ensuring message persistence and durability.
* **Metadata Store**:
  * **ETCD as Default**: Provides a reliable and consistent Metadata store for cluster synchronization.
  * **Configurable Options**: Allows customization of metadata storage to fit specific requirements.
* **Subscription Types:**:
  * Supports various subscription types (exclusive, shared, failover) enabling different messaging patterns such as message queueing and pub-sub.
* **Flexible Message Schemas**
  * Supports multiple message schemas (bytes, string, int64, JSON) providing flexibility in message format and structure.
* **Command-Line Interfaces (CLI)**
  * **Danube CLI**: For handling message publishing and consumption.
  * **Danube Admin CLI**: For managing and interacting with the Danube cluster, including broker, namespace, and topic management.

## Clients

Allows single or multiple Producers to publish on the Topic and multiple Subscriptions to consume the messages from the Topic.

![Producers  Consumers](https://dev-state.com/danube_docs/architecture/img/producers_consumers.png "Producers Consumers")

You can combine the [Subscription Type mechanisms](https://dev-state.com/danube_docs/architecture/Queuing_PubSub_messaging/) in order to obtain message queueing or fan-out pub-sub messaging systems.

Currently, the Danube client libraries are written in:

* [Rust Client](https://crates.io/crates/danube-client) - the Rust [examples](danube-client/examples/) on how to create and use the Producers / Consumers
* [Go Client](https://pkg.go.dev/github.com/danrusei/danube-go) - the Go [examples](https://github.com/danrusei/danube-go/tree/main/examples) on how to create and use the Producers / Consumers

### Community supported clients

Contributions in other languages, such as Python, Java, etc., are also greatly appreciated. If there are any I'll add in this section.

## Development environment

Continuously working on enhancing and adding new features.

**Contributions are welcome**, check [the open issues](https://github.com/danrusei/danube/issues) or report a bug you encountered or a needed feature.

The crates part of the Danube workspace:

* danube-broker - The main crate, danube pubsub platform
  * danube-reliable-dispatch - Part of danube-broker, responsible of reliable dispatching
  * danube-metadata-store - Part of danube-broker, responsibile of Metadata storage
* danube-client - An async Rust client library for interacting with Danube Pub/Sub messaging platform
* danube-cli - Client CLI to handle message publishing and consumption
* danube-admin-cli - Admin CLI designed for interacting with and managing the Danube cluster

[Follow the instructions](https://dev-state.com/danube_docs/development/dev_environment/) on how to setup the development environment.
