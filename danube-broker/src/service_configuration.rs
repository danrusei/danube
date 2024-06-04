#[derive(Debug)]
pub(crate) struct ServiceConfiguration {
    /// Broker Service Address for serving gRPC requests."
    pub(crate) cluster_name: String,
    pub(crate) broker_addr: std::net::SocketAddr,
    pub(crate) meta_store_addr: Option<String>,
}
