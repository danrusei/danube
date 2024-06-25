# Danube

Danube is an open-source distributed Pub/Sub and Streaming platform (inspired by Apache Pulsar).

<img src="docs/pictures/work_in_progress.png " width="250" height="80" alt="Work in Progress">

Check-out [the Docs](docs/) for more details of the Danube Architecture and the supported concepts.

For the moment the Danube system supports only **Non-persistent messaging**, meaning that the messages reside only in memory and are distributed immediately to consumers if available, using a dispatch mechanism based on the subscription types.

## Clients

Allows single or multiple Producers to publish on the Topic and multiple Subscriptions to consume the messages from the Topic.

![Producers  Consumers](docs/pictures/producers_consumers.png "Producers Consumers")

You can combine the [Subscription Type mechanisms](docs/04-Queuing_PubSub_messaging.md) in order to obtain message queueing or fan-out pub-sub messaging systems.

Check-out [the examples](danube-client/examples/) on how to create and use Producers and Consumers.  The client is written in Rust, a GO client will be available once the Danube system reach the alpha stage.

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
