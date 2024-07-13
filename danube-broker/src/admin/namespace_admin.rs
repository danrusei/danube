use crate::admin::DanubeAdminImpl;
use crate::admin_proto::{
    namespace_admin_server::NamespaceAdmin, NamespaceRequest, NamespaceResponse, PolicyResponse,
    TopicListResponse,
};
use crate::policies::Policies;

use tonic::{Request, Response};
use tracing::{trace, Level};

#[tonic::async_trait]
impl NamespaceAdmin for DanubeAdminImpl {
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_namespace_topics(
        &self,
        request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<TopicListResponse>, tonic::Status> {
        trace!("Admin: get the list of topics of a namespace");

        let req = request.into_inner();

        let topics = self
            .resources
            .namespace
            .get_topics_for_namespace(&req.name)
            .await;

        let response = TopicListResponse { topics };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_namespace_policies(
        &self,
        request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<PolicyResponse>, tonic::Status> {
        trace!("Admin: get the configuration policies of a namespace");

        let req = request.into_inner();

        let policies = match self.resources.namespace.get_policies(&req.name) {
            Ok(policies) => policies,
            Err(err) => {
                trace!(
                    "Unable to retrieve polices for the namespace {} due to {}",
                    req.name,
                    err
                );
                Policies::new()
            }
        };

        let serialized = match serde_json::to_string(&policies) {
            Ok(serialized) => serialized,
            Err(err) => format!("Unable to serialize the policies: {}", err),
        };

        let response = PolicyResponse {
            policies: serialized,
        };
        Ok(tonic::Response::new(response))
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
