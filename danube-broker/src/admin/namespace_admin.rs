use crate::admin::DanubeAdminImpl;
use crate::admin_proto::{
    namespace_admin_server::NamespaceAdmin, NamespaceRequest, NamespaceResponse, PolicyResponse,
    TopicListResponse,
};

use tonic::{Request, Response};
use tracing::Level;

#[tonic::async_trait]
impl NamespaceAdmin for DanubeAdminImpl {
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_namespace_topics(
        &self,
        _request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<TopicListResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_namespace_policies(
        &self,
        _request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<PolicyResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_namespace(
        &self,
        _request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<NamespaceResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn delete_namespace(
        &self,
        _request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<NamespaceResponse>, tonic::Status> {
        todo!()
    }
}
