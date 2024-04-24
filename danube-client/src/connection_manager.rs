use crate::{
    errors::{DanubeError, Result},
    rpc_connection::RpcConnection,
};

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

#[derive(Debug, Clone, Default)]
pub struct ConnectionOptions {
    max_connections_per_host: i32,
    keep_alive_interval: Duration,
    connection_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct ConnectionManager {
    connections: Arc<Mutex<HashMap<BrokerAddress, ConnectionStatus>>>,
    connection_options: ConnectionOptions,
}

impl ConnectionManager {
    pub fn new(connection_options: ConnectionOptions) -> Self {
        ConnectionManager {
            connections: Arc::new(Mutex::new(HashMap::new())),
            connection_options,
        }
    }
}
