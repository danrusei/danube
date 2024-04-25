use crate::{
    errors::{DanubeError, Result},
    rpc_connection::{new_rpc_connection, RpcConnection},
};

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};
use url::Url;

/// holds connection information for a broker
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct BrokerAddress {
    /// URL we're using for connection (can be the proxy's URL)
    pub connect_url: Url,
    /// Danube URL for the broker we're actually contacting
    pub broker_url: Url,
    /// true if the connection is through a proxy
    pub proxy: bool,
}

#[derive(Debug, Clone)]
enum ConnectionStatus {
    Connected(RpcConnection),
    Disconnected,
}

#[derive(Debug, Clone, Default)]
pub struct ConnectionOptions {
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

    pub async fn get_connection(
        &self,
        broker_url: &Url,
        connect_url: &Url,
    ) -> Result<RpcConnection> {
        let mut proxy = false;
        if broker_url == connect_url {
            proxy = true;
        }
        let broker = BrokerAddress {
            connect_url: connect_url.clone(),
            broker_url: broker_url.clone(),
            proxy,
        };

        let mut cnx = self.connections.lock().unwrap();

        let entry = cnx.entry(broker);

        match entry {
            Entry::Occupied(mut occupied_entry) => match occupied_entry.get() {
                ConnectionStatus::Connected(rpc_cnx) => Ok(rpc_cnx.to_owned()),
                ConnectionStatus::Disconnected => {
                    let new_rpc_cnx =
                        new_rpc_connection(&self.connection_options, connect_url.to_string())
                            .await?;
                    *occupied_entry.get_mut() = ConnectionStatus::Connected(new_rpc_cnx.clone());
                    Ok(new_rpc_cnx)
                }
            },
            Entry::Vacant(vacant_entry) => {
                let new_rpc_cnx =
                    new_rpc_connection(&self.connection_options, connect_url.to_string()).await?;
                vacant_entry.insert(ConnectionStatus::Connected(new_rpc_cnx.clone()));
                Ok(new_rpc_cnx)
            }
        }
    }
}
