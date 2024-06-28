# Danube

Danube is an open-source distributed Pub/Sub messaging platform (inspired by Apache Pulsar).

Check-out [the Docs](docs/) for more details of the Danube Architecture and the supported concepts.

**⚠️ The messsaging platform is currently under active development and may have missing or incomplete functionalities. Use with caution.**

I'm continuously working on enhancing and adding new features. **Contributions are welcome**, and you can also report any issues you encounter. The client library is currently written in Rust, with a Go client potentially coming soon. Contributions in other languages, such as Python, Java, etc., are also greatly appreciated.

Currently, the Danube system exclusively supports Non-persistent messaging. This means messages reside solely in memory and are promptly distributed to consumers if they are available, utilizing a dispatch mechanism based on subscription types.

## Clients

Allows single or multiple Producers to publish on the Topic and multiple Subscriptions to consume the messages from the Topic.

![Producers  Consumers](docs/pictures/producers_consumers.png "Producers Consumers")

You can combine the [Subscription Type mechanisms](docs/04-Queuing_PubSub_messaging.md) in order to obtain message queueing or fan-out pub-sub messaging systems.

Check-out [the examples](danube-client/examples/) on how to create and use Producers and Consumers.  The client is written in Rust. A GO client may arive soon. However

## Development environment

Create the etcd instance:

```bash
make etcd
```

Use `etcdctl` to inspect metadata in ETCD instance. Export environment variables:

```bash
export ETCDCTL_API=3
export ETCDCTL_ENDPOINTS=http://localhost:2379
```

Run one single Broker instance:

```bash
RUST_LOG=danube_broker=trace target/debug/danube-broker --cluster-name MY_cluster --meta-store-addr 127.0.0.1:2379
```

Run multiple Broker instances:

```bash
make brokers RUST_LOG=danube_broker=trace
```
