use crate::policies::Policies;

use serde::{Deserialize, Serialize};

/// configuration settings for the Danube broker service
/// includes various parameters that control the behavior and performance of the broker
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ServiceConfiguration {
    /// Broker Service Address for serving gRPC requests."
    pub(crate) cluster_name: String,
    pub(crate) broker_addr: String,
    pub(crate) admin_addr: String,
    pub(crate) meta_store_addr: Option<String>,
    pub(crate) prom_exporter: Option<String>,
    pub(crate) bootstrap_namespaces: Vec<String>,
    pub(crate) policies: Policies,
}
