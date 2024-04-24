use crate::{errors::Result, rpc_connection::RpcConnection};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use url::Url;

/// holds connection information for a broker
#[derive(Debug, Clone)]
pub struct BrokerAddress {
    /// URL we're using for connection (can be the proxy's URL)
    pub url: Url,
    /// Danube URL for the broker we're actually contacting must follow the IP:port format
    pub broker_url: String,
    /// true if the connection is through a proxy
    pub proxy: bool,
}

#[derive(Debug, Clone)]
enum ConnectionStatus {
    Connected(Arc<RpcConnection>),
    Disconnected,
}

#[derive(Debug, Clone)]
pub struct ConnectionManager {
    pub url: Url,
    connections: Arc<Mutex<HashMap<BrokerAddress, ConnectionStatus>>>,
    connection_retry_options: ConnectionRetryOptions,
    pub(crate) operation_retry_options: OperationRetryOptions,
}

impl ConnectionManager {
    pub async fn new() -> Result<Self> {
        todo!()
    }
}

/// configuration for reconnection exponential back off
#[derive(Debug, Clone)]
pub struct ConnectionRetryOptions {
    /// minimum delay between connection retries
    pub min_backoff: Duration,
    /// maximum delay between reconnection retries
    pub max_backoff: Duration,
    /// maximum number of connection retries
    pub max_retries: u32,
    /// time limit to establish a connection
    pub connection_timeout: Duration,
    /// keep-alive interval for each broker connection
    pub keep_alive: Duration,
}

impl Default for ConnectionRetryOptions {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn default() -> Self {
        ConnectionRetryOptions {
            min_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(30),
            max_retries: 12u32,
            connection_timeout: Duration::from_secs(10),
            keep_alive: Duration::from_secs(60),
        }
    }
}

/// configuration for Danube operation retries
#[derive(Debug, Clone)]
pub struct OperationRetryOptions {
    /// time limit to receive an answer to a Danube operation
    pub operation_timeout: Duration,
    /// delay between operation retries after a ServiceNotReady error
    pub retry_delay: Duration,
    /// maximum number of operation retries. None indicates infinite retries
    pub max_retries: Option<u32>,
}

impl Default for OperationRetryOptions {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn default() -> Self {
        OperationRetryOptions {
            operation_timeout: Duration::from_secs(30),
            retry_delay: Duration::from_secs(5),
            max_retries: None,
        }
    }
}
