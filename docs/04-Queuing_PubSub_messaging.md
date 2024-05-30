# Pub-Sub messaging

**Danube** is built on the [publish-subscribe](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) pattern. In this pattern, producers publish messages to topics; consumers subscribe to those topics, process incoming messages, and send acknowledgments to the broker when processing is finished.

## Messages

It is the basic unit, they are what producers publish to topics and what consumers then consume from topics.

Structure:

* **Value / data payload** - The data carried by the message. The messages should contain raw bytes, and the schema, serialization / deserialization should be managed by producers / consumers.
* **Properties** - An optional key/value map of user-defined properties.
* **Producer name** - The name of the producer who produces the message.
* **Topic name** - The name of the topic that the message is published to.
* **Sequence ID** - Each message belongs to an ordered sequence on its topic..
* **Publish time** - The timestamp of when the message is published.

The default max size of a message is XX MB, that can be configured.

### Acknowledgment

A message acknowledgment is sent by a consumer to a broker after the consumer consumes a message successfully. In the Persistent mode the consumed message will be permanently stored and deleted only after all the subscriptions have acknowledged it.

### Acknowledgment timeout

In the Persistent mode, the acknowledgment timeout mechanism allows you to set a time range during which the client tracks the unacknowledged messages. After this acknowledgment timeout (ackTimeout) period, the client sends redeliver unacknowledged messages request to the broker, thus the broker resends the unacknowledged messages to the consumer.

## Topics

A topic is a unit of storage that organizes messages into a stream. As in other pub-sub systems, topics are named channels for transmitting messages from producers to consumers. Topic names are URLs that have a well-defined structure:

### /{namespace}/{topic_name}

Example: **/default/markets** (where *default* is the namespace and *markets* the topic)

## Subscriptions

### Pub-Sub or Queuing

* If you want to achieve **message queuing** among consumers, **share the same subscription name** among multiple consumers
* If you want to achieve traditional **fan-out pub-sub messaging** among consumers, **specify a unique subscription name for each consumer** with an exclusive subscription type.

**A Danube subscription** is a named configuration rule that determines how messages are delivered to consumers. It is a lease on a topic established by a group of consumers:

* **Exclusive (can be used for pub-sub)** - The exclusive type is a subscription type that only allows a single consumer to attach to the subscription. If multiple consumers subscribe to a topic using the same subscription, an error occurs.
* **Shared (for queuing)** - The shared subscription type Danube allows multiple consumers to attach to the same subscription. Messages are delivered in a round-robin distribution across consumers, and any given message is delivered to only one consumer.

### Multi-topic subscriptions

Not intended to be supported soon !.. using regex subscription.

## Partitioned topics

### Not Yet Implemented

Normal topics are served only by a single broker, which limits the maximum throughput of the topic. Partitioned topic is a special type of topic handled by multiple brokers, thus allowing for higher throughput.

A partitioned topic is implemented as N internal topics, where N is the number of partitions. When publishing messages to a partitioned topic, each message is routed to one of several brokers. The distribution of partitions across brokers is handled automatically.

![Partitioned Topics](pictures/partitioned_topics.png "Partitioned topics")

Messages for the topic are broadcast to two consumers. The **routing mode** determines each message should be published to which partition, while the **subscription type** determines which messages go to which consumers.

### Routing modes

When publishing to partitioned topics, you must specify a routing mode. The routing mode determines each message should be published to which partition or which internal topic.

* **RoundRobinPartition** - The producer will publish messages across all partitions in round-robin fashion to achieve maximum throughput. If a key is specified on the message, the partitioned producer will **hash the key and assign message to a particular partition**.
* **SinglePartition** - If no key is provided, the producer will randomly pick one single partition and publish all the messages into that partition. While if a key is specified on the message, the partitioned producer will hash the key and assign message to a particular partition.

=============================

## Queuing vs Pub-Sub

Below is some general documentation about Queuing and Pub-Sub, not related to Danube implementation, but just to ensure we are aware of the standards.

Messaging queuing and pub-sub are both messaging patterns used for asynchronous communication between applications, but they differ in their approach:

### Queuing Messaging

* **Model**: Point-to-Point (One-to-One). A message producer sends a message to a specific queue, and only one consumer can receive and process that message.
* **Order**: Messages are typically processed in the order they are received (FIFO - First-In-First-Out). This ensures tasks are completed sequentially.
* **Delivery**: Messages are guaranteed to be delivered at least once. This reliability is crucial for critical tasks.

Examples: Use cases include processing orders, sending emails, or handling failed transactions.

### Pub-Sub Messaging

* **Model**: Publish-Subscribe (One-to-Many). A producer publishes messages to a topic, and any interested subscribers can receive the message. Multiple subscribers can receive the same message.
* **Order**: Message order is not guaranteed. Subscribers receive messages as they are published. This is suitable for real-time updates or notifications.
* **Delivery**: Delivery is often "fire-and-forget," meaning there's no guarantee a subscriber receives the message. This is acceptable for non-critical data.

Examples: Use cases include broadcasting stock price updates, sending chat messages, or triggering real-time analytics.

In Summary:

* **Messaging queues** are for reliable, ordered delivery to a single consumer, ideal for task processing.
* **Pub-sub** is for broadcasting messages to many interested parties, good for real-time updates.
