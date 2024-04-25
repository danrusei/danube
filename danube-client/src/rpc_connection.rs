use tonic::transport::channel::Endpoint;
use url::Url;

use crate::proto::danube_client;
use crate::{connection_manager::ConnectionOptions, errors::Result};

#[derive(Debug, Clone)]
pub(crate) struct RpcConnection {
    id: u64,
    url: String,
}

pub(crate) async fn new_rpc_connection(
    cnx_options: &ConnectionOptions,
    connect_url: String,
) -> Result<RpcConnection> {
    let endpoint = Endpoint::from_shared(String::from(connect_url))?;
    let mut client = danube_client::DanubeClient::connect(endpoint).await?;
    todo!()
}
