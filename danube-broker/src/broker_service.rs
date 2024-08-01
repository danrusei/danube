use anyhow::{anyhow, Result};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Code, Status};
use tracing::{info, warn};

use crate::proto::{ErrorType, Schema as ProtoSchema};

use crate::{
    consumer::Consumer, error_message::create_error_status, policies::Policies, producer::Producer,
    resources::Resources, subscription::SubscriptionOptions, topic::Topic, utils::get_random_id,
};

// BrokerService - owns the topics and manages their lifecycle.
// It also facilitates the creation of producers, subscriptions, and consumers,
// ensuring that producers can publish messages to topics and consumers can consume messages from topics.
#[derive(Debug)]
pub(crate) struct BrokerService {
    // the id of the broker
    pub(crate) broker_id: u64,
    // to handle the metadata and configurations
    pub(crate) resources: Resources,
    // maps topic_name to Topic
    pub(crate) topics: HashMap<String, Topic>,
    // maps producer_id to topic_name
    pub(crate) producer_index: HashMap<u64, String>,
    // maps consumer_id to (topic_name, subscription_name)
    pub(crate) consumer_index: HashMap<u64, (String, String)>,
}

impl BrokerService {
    pub(crate) fn new(resources: Resources) -> Self {
        let broker_id = get_random_id();
        BrokerService {
            broker_id,
            resources: resources,
            topics: HashMap::new(),
            producer_index: HashMap::new(),
            consumer_index: HashMap::new(),
        }
    }

    // The broker checks if it is the owner of the topic. If it is not, but the topic exist in the cluster,
    // the broker instruct the client to redo the lookup request.
    //
    // If the topic doesn't exist in the cluster, and the auto-topic creation is enabled,
    // the broker creates new topic to the metadata store.
    //
    // The Leader Broker will be informed about the new topic creation and assign the topic to a broker.
    // The selected Broker will be informed through watch mechanism and will host the topic.
    pub(crate) async fn get_topic(
        &mut self,
        topic_name: &str,
        schema: Option<ProtoSchema>,
        create_if_missing: bool,
    ) -> Result<bool, Status> {
        // The topic format is /{namespace_name}/{topic_name}
        if !validate_topic_format(topic_name) {
            let error_string =
                "The topic has an invalid format, should be: /namespace_name/topic_name";
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::InvalidTopicName,
                error_string,
                None,
            );
            return Err(status);
        }

        // check if topic is served by this broker
        if self.topics.contains_key(topic_name) {
            return Ok(true);
        }

        let ns_name = get_nsname_from_topic(topic_name);

        // check if Topic already exist in the namespace, if exist, inform the client to redo the Lookup request
        if self
            .resources
            .namespace
            .check_if_topic_exist(ns_name, topic_name)
        {
            let error_string = "The topic exist on the namespace but it is served by another broker, redo the Lookup request";
            let status = create_error_status(
                Code::Unavailable,
                ErrorType::ServiceNotReady,
                error_string,
                None,
            );
            return Err(status);
        }

        // If the topic does not exist and create_if_missing is false
        if !create_if_missing {
            let error_string = &format!("Unable to find the topic: {}", topic_name);
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::TopicNotFound,
                error_string,
                None,
            );
            return Err(status);
        };

        match self.create_topic_cluster(topic_name, schema).await {
            Ok(()) => {
                let error_string =
            "The topic metadata was created, need to redo the lookup to find the correct broker";
                let status = create_error_status(
                    Code::Unavailable,
                    ErrorType::ServiceNotReady,
                    error_string,
                    None,
                );
                return Err(status);
            }
            Err(err) => return Err(err),
        }
    }

    // get all the topics currently served by the Broker
    pub(crate) fn get_topics(&self) -> Vec<&String> {
        let topics: Vec<&String> = self.topics.iter().map(|topic| topic.0).collect();
        topics
    }

    // Creates a topic on the cluster
    // and leave to the Leader Broker to assign to one of the active brokers
    pub(crate) async fn create_topic_cluster(
        &mut self,
        topic_name: &str,
        schema: Option<ProtoSchema>,
    ) -> Result<(), Status> {
        // The topic format is /{namespace_name}/{topic_name}
        if !validate_topic_format(topic_name) {
            let error_string =
                "The topic has an invalid format, should be: /namespace_name/topic_name";
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::InvalidTopicName,
                error_string,
                None,
            );
            return Err(status);
        }

        if schema.is_none() {
            let error_string = "Unable to create a topic without specifying the Schema";
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::UnknownError,
                error_string,
                None,
            );
            return Err(status);
        }

        let ns_name = get_nsname_from_topic(topic_name);

        if let Ok(false) = self.resources.namespace.namespace_exist(ns_name).await {
            let status = Status::unavailable(&format!(
                "Unable to find the namespace {}, the topic can be created only for an exisiting namespace", ns_name
            ));
            return Err(status);
        }

        match self.post_new_topic(topic_name, schema.unwrap(), None).await {
            Ok(()) => return Ok(()),

            Err(err) => {
                let status = Status::internal(&format!(
                    "The broker unable to post the topic to metadata store, due to error: {}",
                    err,
                ));
                return Err(status);
            }
        };
    }

    // post the Topic resources to Metadata Store
    pub(crate) async fn post_new_topic(
        &mut self,
        topic_name: &str,
        schema: ProtoSchema,
        policies: Option<Policies>,
    ) -> Result<()> {
        // store the topic to unassigned queue for the Load Manager to assign to a broker
        // Load Manager will decide which broker is going to serve the new created topic
        // so it will not be added to local list, yet.
        self.resources
            .cluster
            .new_unassigned_topic(topic_name)
            .await?;

        // store the new topic to namespace path: /namespaces/{namespace}/topics/
        self.resources
            .namespace
            .create_new_topic(topic_name)
            .await?;

        // store new topic policy: /topics/{namespace}/{topic}/policy
        if let Some(policies) = policies {
            self.resources
                .topic
                .add_topic_policy(topic_name, policies)
                .await?;
        }

        // store new topic schema: /topics/{namespace}/{topic}/schema
        self.resources
            .topic
            .add_topic_schema(topic_name, schema.into())
            .await?;

        Ok(())
    }

    // Post topic deletion request to Metadata Store
    pub(crate) async fn post_delete_topic(&mut self, topic_name: &str) -> Result<()> {
        // find the broker owning the topic

        let broker_id = match self
            .resources
            .cluster
            .get_broker_for_topic(topic_name)
            .await
        {
            Some(broker_id) => broker_id,
            None => return Err(anyhow!("Unable to find topic")),
        };

        // Remove the topic from the metadata store in three steps:
        // 1. Remove from assigned broker:
        // this will trigger the watch event on the hosting broker to proceed with the deletion
        self.resources
            .cluster
            .schedule_topic_deletion(&broker_id, topic_name)
            .await?;

        // 2. delete the topic from the namespace (from /namespaces)
        self.resources.namespace.delete_topic(topic_name).await?;

        // 3. delete the topic schema (from /topics)
        self.resources.topic.delete_topic_schema(topic_name).await?;

        Ok(())
    }

    // creates the topic on the local broker
    // assumes that it was received from legitimate sources, like ETCDWatchEvent
    // so we know that the topic was checked before and assigned to this broker by load manager
    pub(crate) async fn create_topic_locally(&mut self, topic_name: &str) -> Result<()> {
        // create the topic,
        let mut new_topic = Topic::new(topic_name);

        // get schema from local_cache
        let schema = self.resources.topic.get_schema(topic_name);
        if schema.is_none() {
            warn!("Unable to create topic without a valid schema");
            return Err(anyhow!("Unable to create topic without a valid schema"));
        }
        let _ = new_topic.add_schema(schema.unwrap());

        // get policies from local_cache
        let policies = self.resources.topic.get_policies(topic_name);

        if let Some(with_policies) = policies {
            let _ = new_topic.policies_update(with_policies);
        } else {
            // get namespace policies
            let parts: Vec<_> = topic_name.split('/').collect();
            let ns_name = format!("/{}", parts[1]);

            let policies = self.resources.namespace.get_policies(&ns_name)?;
            //TODO! for now the namespace polices == topic policies, if they will be different as number of values
            // then I shoud copy field by field
            let _ = new_topic.policies_update(policies);
        }

        self.topics.insert(topic_name.to_string(), new_topic);

        Ok(())
    }

    // deletes the topic
    pub(crate) async fn delete_topic(&mut self, topic_name: &str) -> Result<Topic> {
        let topic = match self.topics.get_mut(topic_name) {
            Some(topic) => topic,
            None => {
                return Err(anyhow!(
                    "The topic {} does not exist on the broker {}",
                    topic_name,
                    self.broker_id
                ))
            }
        };

        // disconnect all the producers/consumers associated to the topic
        let (producers, consumers) = topic.close().await?;

        //maybe wait here for producers / consumers to disconnect

        for producer_id in producers {
            self.producer_index.remove(&producer_id);
        }

        for consumer_id in consumers {
            self.consumer_index.remove(&consumer_id);
        }

        // removing the topic should delete all the resources associated with topic
        match self.topics.remove(topic_name) {
            Some(topic) => {
                info!(
                    "The topic {} was removed from the broker {}",
                    topic.topic_name, self.broker_id
                );
                Ok(topic)
            }
            None => Err(anyhow!(
                "The topic {} it is not owened by broker {}",
                topic_name,
                self.broker_id
            )),
        }
    }

    // search for the broker socket address that serve this topic
    pub(crate) async fn lookup_topic(&self, topic_name: &str) -> Option<(bool, String)> {
        // check if it is served by the this broker
        if self.topics.contains_key(topic_name) {
            return Some((true, "".to_string()));
        }

        // if not search in Local Metadata for the broker that serve the topic
        let broker_id = match self
            .resources
            .cluster
            .get_broker_for_topic(topic_name)
            .await
        {
            Some(broker_id) => broker_id,
            None => return None,
        };

        if let Some(broker_addr) = self.resources.cluster.get_broker_addr(&broker_id) {
            return Some((false, broker_addr));
        }

        None
    }

    pub(crate) fn get_schema(&self, topic_name: &str) -> Option<ProtoSchema> {
        let result = self.resources.topic.get_schema(topic_name);
        if let Some(schema) = result {
            return Some(ProtoSchema::from(schema));
        }
        None
    }

    pub(crate) fn check_if_producer_exist(
        &self,
        topic_name: String,
        producer_name: String,
    ) -> bool {
        // the topic is already checked
        let topic = match self.topics.get(&topic_name) {
            None => return false,
            Some(topic) => topic,
        };
        for producer in topic.producers.values() {
            if producer.producer_name == producer_name {
                return true;
            }
        }
        false
    }

    pub(crate) fn health_producer(&mut self, producer_id: u64) -> bool {
        if let Some(topic) = self.find_topic_by_producer(producer_id) {
            return topic.get_producer_status(producer_id);
        }
        false
    }

    // create a new producer and attach to the topic
    pub(crate) fn create_new_producer(
        &mut self,
        producer_name: &str,
        topic_name: &str,
        producer_access_mode: i32,
    ) -> Result<u64> {
        let producer_id = get_random_id();

        if let Some(topic) = self.topics.get_mut(topic_name) {
            match topic.producers.entry(producer_id) {
                Entry::Vacant(entry) => {
                    entry.insert(Producer::new(
                        producer_id,
                        producer_name.into(),
                        topic_name.into(),
                        producer_access_mode,
                    ));
                }
                Entry::Occupied(entry) => {
                    //let current_producer = entry.get();
                    info!("the requested producer: {} already exists", entry.key());
                    return Err(anyhow!(" the producer already exist"));
                }
            }

            // insert into producer_index for efficient searches and retrievals
            self.producer_index
                .insert(producer_id, topic_name.to_string());
        } else {
            return Err(anyhow!("Unable to find the topic: {}", topic_name));
        }

        Ok(producer_id)
    }

    // finding a Topic by Producer ID
    pub(crate) fn find_topic_by_producer(&mut self, producer_id: u64) -> Option<&Topic> {
        if let Some(topic_name) = self.producer_index.get(&producer_id) {
            self.topics.get(topic_name)
        } else {
            None
        }
    }

    // finding a Consumer by Consumer ID
    pub(crate) fn find_consumer_by_id(&self, consumer_id: u64) -> Option<Arc<Mutex<Consumer>>> {
        if let Some((topic_name, subscription_name)) = self.consumer_index.get(&consumer_id) {
            if let Some(topic) = self.topics.get(topic_name) {
                if let Some(subscription) = topic.subscriptions.get(subscription_name) {
                    return subscription.consumers.get(&consumer_id).cloned();
                }
            }
        }
        None
    }

    pub(crate) async fn health_consumer(&mut self, consumer_id: u64) -> bool {
        if let Some(consumer) = self.find_consumer_by_id(consumer_id) {
            let consumer = consumer.lock().await;
            return consumer.get_status();
        }
        false
    }

    pub(crate) async fn check_if_consumer_exist(
        &self,
        consumer_name: &str,
        subscription_name: &str,
        topic_name: &str,
    ) -> Option<u64> {
        let topic = match self.topics.get(topic_name) {
            None => return None,
            Some(topic) => topic,
        };

        let subscription = match topic.get_subscription(subscription_name) {
            Some(subscr) => subscr,
            None => return None,
        };

        let consumer_id = match subscription.get_consumer_id(consumer_name).await {
            Some(id) => id,
            None => return None,
        };

        Some(consumer_id)
    }

    //validate if the consumer is allowed to create new subscription
    pub(crate) fn allow_subscription_creation(&self, topic_name: impl Into<String>) -> bool {
        // check the topic policies here
        let _topic = self
            .topics
            .get(&topic_name.into())
            .expect("unable to find the topic");

        //TODO! once the policies Topic&Namespace Policies are in place we can verify if it is allowed

        true
    }

    //consumer subscribe to topic
    pub(crate) async fn subscribe(
        &mut self,
        topic_name: &str,
        subscription_options: SubscriptionOptions,
    ) -> Result<u64> {
        if let Some(topic) = self.topics.get_mut(topic_name) {
            //TODO! checkTopicOwnership
            // if it's owened by this instance continue,
            // otherwise communicate to client that it has to do Lookup request, as the topic is not serve by this broker

            let consumer = topic
                .subscribe(topic_name, subscription_options.clone())
                .await?;
            let consumer_id = consumer.lock().await.consumer_id;

            // insert into consumer_index for efficient searches and retrievals
            self.consumer_index.insert(
                consumer_id,
                (
                    topic_name.to_string(),
                    subscription_options.subscription_name,
                ),
            );
            return Ok(consumer_id);
        } else {
            return Err(anyhow!("Unable to find the topic: {}", topic_name));
        }
    }

    pub(crate) async fn create_namespace_if_absent(&mut self, namespace_name: &str) -> Result<()> {
        match self
            .resources
            .namespace
            .namespace_exist(namespace_name)
            .await
        {
            Ok(true) => {
                return Err(anyhow!("Namespace {} already exists.", namespace_name));
            }
            Ok(false) => {
                self.resources
                    .namespace
                    .create_namespace(namespace_name, None)
                    .await?;
            }
            Err(err) => {
                return Err(anyhow!("Unable to perform operation {}", err));
            }
        }

        Ok(())
    }

    // deletes only empty namespaces (with no topics)
    pub(crate) async fn delete_namespace(&mut self, ns_name: &str) -> Result<()> {
        self.resources.namespace.delete_namespace(ns_name).await?;

        Ok(())
    }
}

// Topics string representation:  /{namespace}/{topic-name}
pub(crate) fn validate_topic_format(input: &str) -> bool {
    let parts: Vec<&str> = input.split('/').collect();

    if parts.len() != 3 {
        return false;
    }

    for part in parts.iter() {
        if !part
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return false;
        }
    }

    true
}

// extract the namespace from a topic
// example from topic /ns_name/topic_name returns the  ns_name
fn get_nsname_from_topic(topic_name: &str) -> &str {
    // assuming that the topic name has already been validated.
    let parts: Vec<&str> = topic_name.split('/').collect();
    let ns_name = parts.get(1).unwrap();

    ns_name
}
