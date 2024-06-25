use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Policies {
    // Limits the maximum number of producers that can simultaneously publish messages to a specific topic.
    max_producers_per_topic: Option<u32>,
    // Limits the maximum number of subscriptions that can be created on the topic.
    max_subscriptions_per_topic: Option<u32>,
    // Limits the maximum number of consumers that can simultaneously consume messages from a specific topic.
    max_consumers_per_topic: Option<u32>,
    // Limits the maximum number of consumers that can simultaneously use a single subscription on a topic.
    max_consumers_per_subscription: Option<u32>,

    // Defines the Max publish rate (number of messages and/or bytes per second) for producers publishing to the topic.
    max_publish_rate: Option<u32>,
    // Defines the Max dispatch rate (number of messages and/or bytes per second) for the topic.
    max_dispatch_rate: Option<u32>,
    // Defines the dispatch rate for each subscription on the topic.
    max_subscription_dispatch_rate: Option<u32>,

    // Limits the maximum size of a single message that can be published to the topic.
    max_message_size: Option<u32>,
}

impl Policies {
    pub(crate) fn new() -> Self {
        Policies {
            ..Default::default()
        }
    }
    pub(crate) fn get_fields_as_map(&self) -> Map<String, Value> {
        let serialized = serde_json::to_value(self).unwrap();
        let map: Map<String, Value> = serialized.as_object().unwrap().clone();
        map
    }

    // ensure that we can construct a complete Policies with the fields retrieve from the Store
    pub fn from_hashmap(map: HashMap<String, serde_json::Value>) -> Result<Self> {
        let mut policies = Policies::default();
        let mut found_fields = HashSet::new();
        let expected_fields = [
            "max_producers_per_topic",
            "max_subscriptions_per_topic",
            "max_consumers_per_topic",
            "max_consumers_per_subscription",
            "max_publish_rate",
            "max_dispatch_rate",
            "max_subscription_dispatch_rate",
            "max_message_size",
        ];

        for (key, value) in map {
            match key.as_str() {
                "max_producers_per_topic" => {
                    if value.is_null() {
                        policies.max_producers_per_topic = None;
                    } else if let Some(val) = value.as_u64() {
                        policies.max_producers_per_topic = Some(val as u32);
                    }
                    found_fields.insert("max_producers_per_topic");
                }
                "max_consumers_per_topic" => {
                    if value.is_null() {
                        policies.max_consumers_per_topic = None;
                    } else if let Some(val) = value.as_u64() {
                        policies.max_consumers_per_topic = Some(val as u32);
                    }
                    found_fields.insert("max_consumers_per_topic");
                }
                "max_consumers_per_subscription" => {
                    if value.is_null() {
                        policies.max_consumers_per_subscription = None;
                    } else if let Some(val) = value.as_u64() {
                        policies.max_consumers_per_subscription = Some(val as u32);
                    }
                    found_fields.insert("max_consumers_per_subscription");
                }
                "max_subscriptions_per_topic" => {
                    if value.is_null() {
                        policies.max_subscriptions_per_topic = None;
                    } else if let Some(val) = value.as_u64() {
                        policies.max_subscriptions_per_topic = Some(val as u32);
                    }
                    found_fields.insert("max_subscriptions_per_topic");
                }
                "max_publish_rate" => {
                    if value.is_null() {
                        policies.max_publish_rate = None;
                    } else if let Some(val) = value.as_u64() {
                        policies.max_publish_rate = Some(val as u32);
                    }
                    found_fields.insert("max_publish_rate");
                }
                "max_dispatch_rate" => {
                    if value.is_null() {
                        policies.max_dispatch_rate = None;
                    } else if let Some(val) = value.as_u64() {
                        policies.max_dispatch_rate = Some(val as u32);
                    }
                    found_fields.insert("max_dispatch_rate");
                }
                "max_subscription_dispatch_rate" => {
                    if value.is_null() {
                        policies.max_subscription_dispatch_rate = None;
                    } else if let Some(val) = value.as_u64() {
                        policies.max_subscription_dispatch_rate = Some(val as u32);
                    }
                    found_fields.insert("max_subscription_dispatch_rate");
                }
                "max_message_size" => {
                    if value.is_null() {
                        policies.max_message_size = None;
                    } else if let Some(val) = value.as_u64() {
                        policies.max_message_size = Some(val as u32);
                    }
                    found_fields.insert("max_message_size");
                }
                _ => {} // Ignore unknown fields
            }
        }

        // Check for missing fields
        let missing_fields: Vec<String> = expected_fields
            .iter()
            .filter(|&&field| !found_fields.contains(field))
            .map(|&field| field.to_string())
            .collect();

        if !missing_fields.is_empty() {
            return Err(anyhow!(
                "Can't construct a Policies struct, as some fields are missing: {:?}",
                missing_fields
            ));
        }

        Ok(policies)
    }
}
