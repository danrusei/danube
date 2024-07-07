use crate::admin::DanubeAdminImpl;
use crate::admin_proto::{
    broker_admin_server::BrokerAdmin, BrokerListResponse, BrokerResponse, Empty,
    NamespaceListResponse,
};

use tonic::{Request, Response};
use tracing::Level;

#[tonic::async_trait]
impl BrokerAdmin for DanubeAdminImpl {
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_brokers(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<BrokerListResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_leader_broker(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<BrokerResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_namespaces(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<NamespaceListResponse>, tonic::Status> {
        todo!()
    }
}
