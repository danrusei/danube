# Danube Pub/Sub Topics

**Danube Streaming** service is designed for high-performance & low-latency messaging. As for now it supports only **Non-Persistent Messaging (Topic)**.

## Non-Persistent Topics

### Producers of Non-Persistent Topics

* **Low Latency**: Messages sent to non-persistent topics are not stored on disk, which reduces the latency associated with producing messages.
* **No Guarantees**: There are no guarantees that messages will be delivered. If the broker crashes or if there are network issues, messages might be lost.
* **Transient Acknowledgements**: Acknowledgements to the producer are quicker since they are based on in-memory operations rather than disk writes.
* **Publishing Without Consumers**: Producers are allowed to publish messages to a non-persistent topic even if there are no active consumers. However, if no consumers are connected, these messages will effectively be dropped because non-persistent topics do not store messages.

### Consumers of Non-Persistent Topics

* **Real-time Consumption**: Consumers of non-persistent topics typically process messages in real-time. If a consumer is not available, the message might be lost.
* **No Replay**: Since messages are not stored, consumers cannot replay messages. They must process them as they arrive.
* **Reduced Overhead**: Non-persistent topics can handle higher throughput with lower overhead, suitable for use cases where occasional message loss is acceptable.
* **Message Delivery**: Messages are delivered to consumers only if they are currently connected and subscribed to the topic. If there are no consumers, the messages are not retained and are discarded by the broker.

### Order of Operations for Non-Persistent Topics

* **Producer Publishes Message**: The producer sends a message to the broker.
* **Broker Receives Message**: The broker processes the message.
* **Consumer Availability Check**: If consumers are available, the message is delivered to them in real-time.
* **No Consumers**: If no consumers are connected, the message is discarded immediately.

## Persistent Topics

### Producers of Persistent Topics

* **Durability**: Messages sent to persistent topics are stored on disk and replicated according to the topic's configuration. This ensures that messages are not lost even if brokers crash.
* **Acknowledgements**: Producers receive acknowledgments once the message is safely stored and replicated. This adds a small amount of latency compared to non-persistent topics.
* **Delivery Guarantees**: Producers can rely on stronger delivery guarantees (e.g., at least once or effectively once).
* **Publishing Without Consumers**: Producers can publish messages to a persistent topic even if there are no active consumers. Unlike non-persistent topics, these messages will be stored by the broker.

### Consumers of Persistent Topics

* **Message Retention**: Consumers can consume messages at any time as long as the retention policy allows. This is useful for replaying messages, ensuring that no messages are missed.
* **Consumption Acknowledgements**: Consumers acknowledge each message, allowing the broker to track which messages have been consumed and manage retention accordingly.
* **Fault Tolerance**: If a consumer crashes, it can resume consumption from where it left off, as the messages are stored persistently on the broker.
* **Message Retention**: Messages are stored according to the configured retention policies. This ensures that even if no consumers are currently connected, the messages will be available for consumption later.

### Order of Operations for Persistent Topics

* **Producer Publishes Message**: The producer sends a message to the broker.
* **Broker Receives and Stores Message**: The broker stores the message on disk and replicates it according to the configuration.
* **Message Acknowledgment**: The broker acknowledges the producer that the message is safely stored.
* **Consumer Availability Check**: If consumers are available, the message is delivered to them.
* **No Consumers**: If no consumers are connected, the message remains stored and is available for future consumption.

## Use Cases

* **Non-Persistent Topics**: Suitable for scenarios where low latency is critical and some message loss is acceptable, such as real-time monitoring, telemetry data, and ephemeral chat messages.
* **Persistent Topics**: Ideal for use cases requiring high reliability and message durability, such as financial transactions, order processing, and logging critical events.

**Note**: Besides the Persistence on Disk and Non-Persistence behaviour as described above, a third option with the messages stored in memory and not persisted to disk is evaluated.
