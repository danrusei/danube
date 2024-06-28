use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use std::any;
use std::collections::{hash_map::Entry, HashMap};
use std::net::SocketAddr;
use std::str::from_boxed_utf8_unchecked;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{transport::Server, Code, Status};
use tracing::{info, warn};
use tracing_subscriber::fmt::format;

use crate::proto::{ErrorType, ProducerAccessMode, Schema as ProtoSchema};

use crate::{
    consumer::Consumer,
    error_message::create_error_status,
    policies::Policies,
    producer::Producer,
    resources::Resources,
    subscription::SubscriptionOptions,
    topic::{self, Topic},
    utils::get_random_id,
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
    // a map with namespace wise topics - Namespace --> topicName --> topic
    //topics: DashMap<String, DashMap<String, Topic>>,
    // maps topic_name to Topic
    pub(crate) topics: HashMap<String, Topic>,
    // the list of active producers, mapping producer_id to topic_name
    pub(crate) producers: HashMap<u64, String>,
    // the list of active consumers
    pub(crate) consumers: HashMap<u64, Arc<Mutex<Consumer>>>,
}

impl BrokerService {
    pub(crate) fn new(resources: Resources) -> Self {
        let broker_id = get_random_id();
        BrokerService {
            broker_id,
            resources: resources,
            topics: HashMap::new(),
            producers: HashMap::new(),
            consumers: HashMap::new(),
        }
    }

    // The broker checks if it is the owner of the topic.
    //
    // If it is not, the broker responds to the client with a redirection message
    // containing the address of the correct broker that owns the topic.
    //  ?????? or is better to ask the client to redo the lookup request????
    //
    // If the topic doesn't exist in the cluster, and auto-topic creation is enabled,
    // the broker creates new topic to the metadata store, that will be assigned by Load Manager to a broker
    // it respond to the client with Service_not_ready, to retry the lookup
    pub(crate) async fn get_topic(
        &mut self,
        topic_name: &str,
        schema: Option<ProtoSchema>,
        create_if_missing: bool,
    ) -> Result<bool, Status> {
        // The topic format is /{namespace_name}/{topic_name}
        if !validate_topic(topic_name) {
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

        let status = match self
            .post_new_topic(ns_name, topic_name, schema.unwrap(), None)
            .await
        {
            Ok(()) => {
                let error_string = "The topic metadata was created, need to redo the lookup to find the correct broker";
                let status = create_error_status(
                    Code::Unavailable,
                    ErrorType::ServiceNotReady,
                    error_string,
                    None,
                );
                status
            }
            Err(err) => {
                let status = Status::internal(&format!(
                    "The broker unable to post the topic to metadata store, due to error: {}",
                    err,
                ));
                status
            }
        };

        return Err(status);
    }

    // get all the topics currently served by the Broker
    pub(crate) fn get_topics(&self) -> Vec<&String> {
        let topics: Vec<&String> = self.topics.iter().map(|topic| topic.0).collect();
        topics
    }

    // post the Topic resources to Metadata Store
    pub(crate) async fn post_new_topic(
        &mut self,
        ns_name: &str,
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

    pub(crate) async fn create_topic(&mut self, topic_name: &str) -> Result<()> {
        // create the topic,
        let mut new_topic = Topic::new(topic_name);

        // get schema from local_cache
        let schema = self.resources.topic.get_schema(topic_name);
        if schema.is_none() {
            warn!("Unable to create topic without a valid schema");
            return Err(anyhow!("Unable to create topic without a valid schema"));
        }
        new_topic.add_schema(schema.unwrap());

        // get policies from local_cache
        let policies = self.resources.topic.get_policies(topic_name);

        if let Some(with_policies) = policies {
            new_topic.policies_update(with_policies);
        } else {
            // get namespace policies
            let parts: Vec<_> = topic_name.split('/').collect();
            let ns_name = format!("/{}", parts[1]);

            let policies = self.resources.namespace.get_policies(&ns_name)?;
            //TODO! for now the namespace polices == topic policies, if they will be different as number of values
            // then I shoud copy field by field
            new_topic.policies_update(policies);
        }

        self.topics.insert(topic_name.to_string(), new_topic);

        Ok(())
    }

    // deletes the topic
    pub(crate) async fn delete_topic(&mut self, topic: &str) -> Result<Topic> {
        todo!()
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
        let topic = self.topics.get(topic_name);
        if let Some(topic) = topic {
            let schema = topic.get_schema();
            if let Some(schema) = schema {
                return Some(ProtoSchema::from(schema));
            }
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

    // create a new producer and attach to the topic
    pub(crate) fn create_new_producer(
        &mut self,
        producer_name: &str,
        topic_name: &str,
        producer_access_mode: i32,
    ) -> Result<u64> {
        let topic = self
            .topics
            .get_mut(topic_name)
            .expect("we know that the topic exist");

        let producer_id = get_random_id();
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

        self.producers
            .entry(producer_id)
            .or_insert(topic_name.into());

        Ok(producer_id)
    }

    // having the producer_id, return to the caller the topic attached to the producer
    pub(crate) fn get_topic_for_producer(&mut self, producer_id: u64) -> Result<&Topic> {
        let topic_name = match self.producers.entry(producer_id) {
            Entry::Vacant(entry) => {
                return Err(anyhow!(
                    "the producer with id {} does not exist",
                    producer_id
                ))
            }
            Entry::Occupied(entry) => entry.get().to_owned(),
        };

        if !self.topics.contains_key(&topic_name) {
            return Err(anyhow!(
                "Unable to find any topic associated to this producer"
            ));
        }

        Ok(self.topics.get_mut(&topic_name).unwrap())
    }

    pub(crate) fn get_consumer(&self, consumer_id: u64) -> Option<Arc<Mutex<Consumer>>> {
        self.consumers.get(&consumer_id).cloned()
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
        let topic = self
            .topics
            .get_mut(topic_name)
            .expect("the topic should be there");

        //TODO! use NameSpace service to checkTopicOwnership
        // if it's owened by this instance continue,
        // otherwise communicate to client that it has to do Lookup request, as the topic is not serve by this broker

        let consumer = topic.subscribe(topic_name, subscription_options).await?;

        let consumer_id = consumer.lock().await.consumer_id;

        self.consumers.entry(consumer_id).or_insert(consumer);

        Ok(consumer_id)
    }
}

// Topics string representation:  /{namespace}/{topic-name}
pub(crate) fn validate_topic(input: &str) -> bool {
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

fn get_nsname_from_topic(topic_name: &str) -> &str {
    // assuming that the topic name has already been validated.
    let parts: Vec<&str> = topic_name.split('/').collect();
    let ns_name = parts.get(1).unwrap();

    ns_name
}
