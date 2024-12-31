# Danube-admin-cli

The danube-admin-cli is a command-line interface designed for interacting with and managing the Danube cluster.

## Commands

✅ - **Implemented**

❌ - **Not Implemented Yet**

### Broker commands

- **danube-admin-cli brokers list** - ✅
  - List active brokers of the cluster

- **danube-admin-cli brokers leader-broker** - ✅
  - Get the information of the leader broker

- **danube-admin-cli brokers namespaces** - ✅
  - List namespaces part of the cluster

### Namespace commands

- **danube-admin-cli namespaces topics** *NAMESPACE* - ✅
  - Get the list of topics of a namespace

- **danube-admin-cli namespaces policies** *NAMESPACE* - ✅
  - Get the configuration policies of a namespace

- **danube-admin-cli namespaces create** *NAMESPACE* - ✅
  - Create a new namespace

- **danube-admin-cli namespaces delete** *NAMESPACE* - ✅
  - Deletes a namespace. The namespace needs to be empty

### Topic Commands

- **danube-admin-cli topics list** *NAMESPACE* - ✅
  - Get the list of topics of a namespace

- **danube-admin-cli topics create** *TOPIC* - ✅
  - Creates a non-partitioned topic

- **danube-admin-cli topics create-partitioned-topic** *TOPIC* - ❌
  - Create a partitioned topic (--partitions #)

- **danube-admin-cli topics delete** *TOPIC* - ✅
  - Delete the topic

- **danube-admin-cli topics unsubscribe** --subscription *SUBSCRIPTION* *TOPIC* - ❌
  - Delete a subscription from a topic

- **danube-admin-cli topics subscriptions** *TOPIC* - ✅
  - Get the list of subscriptions on the topic
  
- **danube-admin-cli topics create-subscription** --subscription *SUBSCRIPTION* *TOPIC* - ❌
  - Create a new subscription for the topic
