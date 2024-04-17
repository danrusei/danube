# Pub-Sub messaging

**Danube** is built on the [publish-subscribe](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) pattern. In this pattern, producers publish messages to topics; consumers subscribe to those topics, process incoming messages, and send acknowledgments to the broker when processing is finished.

When a subscription is created, Danube retains all messages, even if the consumer is disconnected. The retained messages are discarded only when a consumer acknowledges that all these messages are processed successfully.

## Messages

It is the basic unit, they are what producers publish to topics and what consumers then consume from topics.

Structure:

* **Value / data payload** - The data carried by the message. The messages should contain raw bytes ???, and the schema, serialization / deserialization should be managed by producers / consumers.
* **Key** - The key (string type) of the message. It is a short name of message key or partition key.
* **Properties** - An optional key/value map of user-defined properties.
* **Producer name** - The name of the producer who produces the message. (maybe this should be handle by the broker)
* **Topic name** - The name of the topic that the message is published to.
* **Sequence ID** - Each Pulsar message belongs to an ordered sequence on its topic.
* **Message ID** -  The message ID of a message is assigned by storage as soon as the message is persistently stored. Message ID indicates a message's specific position in a ledger and is unique within the cluster.
* **Publish time** - The timestamp of when the message is published.

The default max size of a message is 5 MB, that can be configured.

## Acknowledgment

A message acknowledgment is sent by a consumer to a broker after the consumer consumes a message successfully. Then, this consumed message will be permanently stored and deleted only after all the subscriptions have acknowledged it.
