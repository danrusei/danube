use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Policies {
    #[serde(default = "default_zero")]
    max_producers_per_topic: Option<u32>,
    #[serde(default = "default_zero")]
    max_consumers_per_topic: Option<u32>,
    #[serde(default = "default_zero")]
    max_consumers_per_subscription: Option<u32>,
    #[serde(default = "default_zero")]
    max_subscriptions_per_topic: Option<u32>,
    #[serde(default = "default_zero")]
    max_topics_per_namespace: Option<u32>,
}

fn default_zero() -> Option<u32> {
    Some(0)
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
            "max_consumers_per_topic",
            "max_consumers_per_subscription",
            "max_subscriptions_per_topic",
            "max_topics_per_namespace",
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
                "max_topics_per_namespace" => {
                    if value.is_null() {
                        policies.max_topics_per_namespace = None;
                    } else if let Some(val) = value.as_u64() {
                        policies.max_topics_per_namespace = Some(val as u32);
                    }
                    found_fields.insert("max_topics_per_namespace");
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
