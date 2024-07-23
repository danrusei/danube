use tonic::transport::{Channel, Uri};
use tracing::info;

use crate::{connection_manager::ConnectionOptions, errors::Result};

#[derive(Debug, Clone)]
pub(crate) struct RpcConnection {
    pub(crate) grpc_cnx: Channel,
}

pub(crate) async fn new_rpc_connection(
    cnx_options: &ConnectionOptions,
    connect_url: &Uri,
) -> Result<RpcConnection> {
    info!(
        "Attempting to establish a new RPC connection to {}",
        connect_url
    );

    let mut endpoint = Channel::builder(connect_url.to_owned());

    if let Some(keep_alive_interval) = cnx_options.keep_alive_interval {
        endpoint = endpoint.http2_keep_alive_interval(keep_alive_interval);
    };

    if let Some(connection_timeout) = cnx_options.connection_timeout {
        endpoint = endpoint.connect_timeout(connection_timeout);
    }

    let grpc_cnx = endpoint.connect().await?;

    info!(
        "Successfully established a new RPC connection to {}",
        connect_url
    );

    Ok(RpcConnection { grpc_cnx })

    //Client creation method:
    //
    //let client1 = danube_client::DanubeClient::new(grpc_cnx.clone());
    //let client2 = discovery_client::DiscoveryClient::new(grpc_cnx.clone());
}
