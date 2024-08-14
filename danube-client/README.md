# Danube-client

An async Rust client library for interacting with Danube Pub/Sub messaging platform.

[Danube](https://github.com/danrusei/danube) is an open-source **distributed** Pub/Sub messaging platform written in Rust. Consult [the documentation](https://dev-state.com/danube_docs/) for supported concepts and the platform architecture.

I'm working on improving it and adding new features. Please feel free to contribute or report any issues you encounter.

## Example usage

Check out the [example files](https://github.com/danrusei/danube/tree/main/danube-client/examples).

### Producer

```rust
let client = DanubeClient::builder()
    .service_url("http://127.0.0.1:6650")
    .build()
    .unwrap();

let topic_name = "/default/test_topic";
let producer_name = "test_prod";

let mut producer = client
    .new_producer()
    .with_topic(topic_name)
    .with_name(producer_name)
    .build();

producer.create().await?;
println!("The Producer {} was created", producer_name);

let encoded_data = "Hello Danube".as_bytes().to_vec();

let message_id = producer.send(encoded_data, None).await?;
println!("The Message with id {} was sent", message_id);
```

### Consumer

```rust
let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .unwrap();

    let topic = "/default/test_topic";
    let consumer_name = "test_cons";
    let subscription_name = "test_subs";

    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name(consumer_name)
        .with_subscription(subscription_name)
        .with_subscription_type(SubType::Exclusive)
        .build();

    // Subscribe to the topic
    consumer.subscribe().await?;
    println!("The Consumer {} was created", consumer_name);

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    while let Some(message) = message_stream.recv().await {
        let payload = message.payload;

        let result = String::from_utf8(payload);

        match result {
            Ok(message) => println!("Received message: {:?}", message),
            Err(e) => println!("Failed to convert Payload to String: {}", e),
        }
    }
```

## Contribution

Check [the documentation](https://dev-state.com/danube_docs/) on how to setup a Danube Broker.
