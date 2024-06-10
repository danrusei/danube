# Danube Platform

**Danube Platform** service is designed for high-performance & low-latency messaging. As for now it supports only **Pub/Sub Messaging**.

## Danube Pub/Sub messaging

### Purpose and Use Cases

* **Purpose**: Designed for decoupling producers and consumers, enabling asynchronous communication between different parts of a system.
* **Use Cases**: Event-driven architectures, real-time notifications, decoupled microservices, and distributed systems. Suitable for scenarios where low latency is critical and some message loss is acceptable, such as real-time monitoring, telemetry data, and ephemeral chat messages.

### Architecture and Design

* **Components**: Consists of Producers, Consumers (subscriptions),  and the message broker.
* **Message Flow**: Producers send messages to a broker, which then distributes them to subscribers based on subscription criteria.
* **Scaling**: Scales by adding more brokers or distributing load (topics / partitions) across multiple brokers.

### Data Handling and Processing Models

#### Pub/Sub messaging Producers

* **Low Latency**: Messages sent to topics are not stored on disk, which reduces the latency associated with producing messages.
* **Order Guarantees**: Provide ordering within the topic or partition.
* **Message Delivery**: There are no guarantees that messages will be delivered. If the broker crashes or if there are network issues, messages might be lost.
* **Transient Acknowledgements**: Acknowledgements to the producer are quicker since they are based on in-memory operations rather than disk writes.
* **Publishing Without Consumers**: Producers are allowed to publish messages to topics even if there are no active consumers. However, if no consumers are connected, these messages will effectively be dropped because the topics do not store messages.

#### Pub/Sub messaging Consumers

* **Real-time Consumption**: Consumers of the topics typically process messages in real-time. If a consumer is not available, the message might be lost.
* **No Replay**: Since messages are not stored, consumers cannot replay messages. They must process them as they arrive.
* **Reduced Overhead**: The topics can handle higher throughput with lower overhead, suitable for use cases where occasional message loss is acceptable.
* **Message Delivery**: Messages are delivered to consumers only if they are currently connected and subscribed to the topic. If there are no consumers, the messages are not retained and are discarded by the broker.

#### Order of Operations of Pub/Sub messaging

* **Producer Publishes Message**: The producer sends a message to the broker.
* **Broker Receives Message**: The broker processes the message.
* **Consumer Availability Check**: If consumers are available, the message is delivered to them in real-time.
* **No Consumers**: If no consumers are connected, the message is discarded.

## Danube Streaming (design considerations)

### Purpose and Use Cases

* **Purpose**: Designed for processing and analyzing large volumes of data in real-time as it is generated.
* **Use Cases**: Real-time analytics, data pipelines, event sourcing, continuous data processing, and stream processing applications. Ideal for use cases requiring high reliability and message durability, such as financial transactions, order processing, and logging critical events.

### Architecture and Design

* **Components**: Consists of producers, consumers, stream processors, and a distributed log.
* **Data Flow**: Producers write data to a distributed log, which consumers and stream processors read from in a continuous fashion.
* **Scaling**: Designed to handle high throughput and scale horizontally by partitioning data across multiple nodes in a cluster.

### Data Handling and Processing Models

### Streaming Producers

* **Durability**: Messages sent to topics are stored to persistent storage and replicated according to the topic's configuration. This ensures that messages are not lost even if brokers crash. This allows playback of streams for for historical data analysis and reprocessing.
* **Acknowledgements**: Producers receive acknowledgments once the message is safely stored and replicated. This adds a small amount of latency compared to pub/sub messaging.
* **Order Guarantees**: Provide ordering within the topic or partition.
* **Delivery Guarantees**: Producers can rely on stronger delivery guarantees (e.g., at least once or effectively once).
* **Publishing Without Consumers**: Producers can publish messages to a topic even if there are no active consumers. These messages will be stored by the broker.
* **Processing**: Supports complex processing such as windowed operations, aggregations, joins, and stateful transformations.

### Streaming Consumers

* **Message Retention**: Consumers can consume messages at any time as long as the retention policy allows. This is useful for replaying messages, ensuring that no messages are missed.
* **Consumption Acknowledgements**: Consumers acknowledge each message, allowing the broker to track which messages have been consumed and manage retention accordingly.
* **Fault Tolerance**: If a consumer crashes, it can resume consumption from where it left off, as the messages are stored persistently on the broker.
* **Message Retention**: Messages are stored according to the configured retention policies. This ensures that even if no consumers are currently connected, the messages will be available for consumption later.

### Order of Operations of Streaming

* **Producer Publishes Message**: The producer sends a message to the broker.
* **Broker Receives and Stores Message**: The broker stores the message on the persistent storage and replicates it according to the configuration.
* **Message Acknowledgment**: The broker acknowledges the producer that the message is safely stored.
* **Consumer Availability Check**: If consumers are available, the message is delivered to them.
* **No Consumers**: If no consumers are connected, the message remains stored and is available for future consumption.
