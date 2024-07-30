# Danube-admin

The danube-admin is a command-line interface designed for interacting with and managing the Danube cluster.

## Commands

✅ - **Implemented**

❌ - **Not Implemented Yet**

### Broker commands

- **danube-admin brokers list** - ✅
  - List active brokers of the cluster

- **danube-admin brokers leader-broker** - ✅
  - Get the information of the leader broker

- **danube-admin brokers namespaces** - ✅
  - List namespaces part of the cluster

### Namespace commands

- **danube-admin namespaces topics** *NAMESPACE* - ✅
  - Get the list of topics of a namespace

- **danube-admin namespaces policies** *NAMESPACE* - ✅
  - Get the configuration policies of a namespace

- **danube-admin namespaces create** *NAMESPACE* - ✅
  - Create a new namespace

- **danube-admin namespaces delete** *NAMESPACE* - ✅
  - Deletes a namespace. The namespace needs to be empty

### Topic Commands

- **danube-admin topics list** *NAMESPACE* - ✅
  - Get the list of topics of a namespace

- **danube-admin topics create** *TOPIC* - ✅
  - Creates a non-partitioned topic

- **danube-admin topics create-partitioned-topic** *TOPIC* - ❌
  - Create a partitioned topic (--partitions #)

- **danube-admin topics delete** *TOPIC* - ✅
  - Delete the topic

- **danube-admin topics unsubscribe** --subscription *SUBSCRIPTION* *TOPIC* - ❌
  - Delete a subscription from a topic

- **danube-admin topics subscriptions** *TOPIC* - ✅
  - Get the list of subscriptions on the topic
  
- **danube-admin topics create-subscription** --subscription *SUBSCRIPTION* *TOPIC* - ❌
  - Create a new subscription for the topic
