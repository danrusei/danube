# Danube-cli

The danube-cli is a command-line interface designed for interacting with and managing the Danube cluster.

## Commands

✅ - **Implemented**

❌ - **Not Implemented**

### Broker commands

- **danube-cli brokers list** - ✅
  - List active brokers of the cluster

- **danube-cli brokers leader-broker** - ✅
  - Get the information of the leader broker

- **danube-cli brokers namespaces** - ✅
  - List namespaces part of the cluster

### Namespace commands

- **danube-cli namespaces topics** *NAMESPACE* - ❌
  - Get the list of topics of a namespace

- **danube-cli namespaces policies** *NAMESPACE* - ❌
  - Get the configuration policies of a namespace

- **danube-cli namespaces create** *NAMESPACE* - ❌
  - Create a new namespace

- **danube-cli namespaces delete** *NAMESPACE* - ❌
  - Deletes a namespace. The namespace needs to be empty

### Topic Commands

- **danube-cli topics list** *NAMESPACE* - ❌
  - Get the list of topics of a namespace

- **danube-cli topics create** *TOPIC* - ❌
  - Creates a non-partitioned topic

- **danube-cli topics create-partitioned-topic** *TOPIC* - ❌
  - Create a partitioned topic (--partitions #)

- **danube-cli topics delete** *TOPIC* - ❌
  - Delete the topic

- **danube-cli topics unsubscribe** --subscription *SUBSCRIPTION* *TOPIC* - ❌
  - Delete a subscription from a topic

- **danube-cli topics subscriptions** *TOPIC* - ❌
  - Get the list of subscriptions on the topic
  
- **danube-cli topics create-subscription** --subscription *SUBSCRIPTION* *TOPIC* - ❌
  - Create a new subscription for the topic
