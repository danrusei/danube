#[derive(Debug)]
pub(crate) struct ServiceConfiguration {
    /// Broker Service Address for serving gRPC requests."
    pub(crate) broker_addr: std::net::SocketAddr,
}
