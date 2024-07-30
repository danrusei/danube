# Danube

Danube is an open-source distributed Pub/Sub messaging platform (inspired by Apache Pulsar).

Check-out [the Docs](https://dev-state.com/danube_docs/) for more details of the Danube Architecture and the supported concepts.

Currently, the Danube system exclusively supports Non-persistent messaging. This means messages reside solely in memory and are promptly distributed to consumers if they are available, utilizing a dispatch mechanism based on subscription types.

## Clients

Allows single or multiple Producers to publish on the Topic and multiple Subscriptions to consume the messages from the Topic.

![Producers  Consumers](https://dev-state.com/danube_docs/architecture/img/producers_consumers.png "Producers Consumers")

You can combine the [Subscription Type mechanisms](docs/04-Queuing_PubSub_messaging.md) in order to obtain message queueing or fan-out pub-sub messaging systems.

Check-out [the examples](danube-client/examples/) on how to create and use Producers and Consumers.  The client is written in Rust. A GO client may arive soon.

**⚠️ The messsaging platform is currently under active development and may have missing or incomplete functionalities. Use with caution.**

## Development environment

I'm continuously working on enhancing and adding new features. **Contributions are welcome**, and you can also report any issues you encounter. The client library is currently written in Rust, with a Go client potentially coming soon. Contributions in other languages, such as Python, Java, etc., are also greatly appreciated.

If you want to contribute to any of the following crates:

* danube-broker - The main crate, danube pubsub platform
* danube-admin - Admin CLI designed for interacting with and managing the Danube cluster
* danube-client - An async Rust client library for interacting with Danube Pub/Sub messaging platform
* danube-pubsub - CLI to handle message publishing and consumption,

[Follow the instructions](https://dev-state.com/danube_docs/development/dev_environment/) on how to setup the development environment.
