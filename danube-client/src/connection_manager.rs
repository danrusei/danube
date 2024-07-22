use crate::{
    errors::Result,
    rpc_connection::{new_rpc_connection, RpcConnection},
};

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use tonic::transport::Uri;

/// holds connection information for a broker
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct BrokerAddress {
    /// URL we're using for connection (can be the proxy's URL)
    pub connect_url: Uri,
    /// Danube URL for the broker we're actually contacting
    pub broker_url: Uri,
    /// true if the connection is through a proxy
    pub proxy: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ConnectionStatus {
    Connected(Arc<RpcConnection>),
    Disconnected,
}

#[derive(Debug, Clone, Default)]
pub struct ConnectionOptions {
    pub(crate) keep_alive_interval: Option<Duration>,
    pub(crate) connection_timeout: Option<Duration>,
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

    pub async fn get_connection(
        &self,
        broker_url: &Uri,
        connect_url: &Uri,
    ) -> Result<Arc<RpcConnection>> {
        let mut proxy = false;
        if broker_url == connect_url {
            proxy = true;
        }
        let broker = BrokerAddress {
            connect_url: connect_url.clone(),
            broker_url: broker_url.clone(),
            proxy,
        };

        let mut cnx = self.connections.lock().await;

        // let entry = cnx.entry(broker);

        match cnx.entry(broker) {
            Entry::Occupied(mut occupied_entry) => match occupied_entry.get() {
                ConnectionStatus::Connected(rpc_cnx) => Ok(rpc_cnx.clone()),
                ConnectionStatus::Disconnected => {
                    let new_rpc_cnx =
                        new_rpc_connection(&self.connection_options, connect_url).await?;
                    let rpc_cnx = Arc::new(new_rpc_cnx);
                    *occupied_entry.get_mut() = ConnectionStatus::Connected(rpc_cnx.clone());
                    Ok(rpc_cnx)
                }
            },
            Entry::Vacant(vacant_entry) => {
                let new_rpc_cnx = new_rpc_connection(&self.connection_options, connect_url).await?;
                let rpc_cnx = Arc::new(new_rpc_cnx);
                vacant_entry.insert(ConnectionStatus::Connected(rpc_cnx.clone()));
                Ok(rpc_cnx)
            }
        }
    }
}
