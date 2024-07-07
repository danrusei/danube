use crate::admin::DanubeAdminImpl;
use crate::admin_proto::{
    topic_admin_server::TopicAdmin, NamespaceRequest, PartitionedTopicRequest,
    SubscriptionListResponse, SubscriptionRequest, SubscriptionResponse, TopicListResponse,
    TopicRequest, TopicResponse,
};

use tonic::{Request, Response};
use tracing::Level;

#[tonic::async_trait]
impl TopicAdmin for DanubeAdminImpl {
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_topics(
        &self,
        _request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<TopicListResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_topic(
        &self,
        _request: Request<TopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_partitioned_topic(
        &self,
        _request: Request<PartitionedTopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn delete_topic(
        &self,
        _request: Request<TopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn unsubscribe(
        &self,
        _request: Request<SubscriptionRequest>,
    ) -> std::result::Result<Response<SubscriptionResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_subscriptions(
        &self,
        _request: Request<TopicRequest>,
    ) -> std::result::Result<Response<SubscriptionListResponse>, tonic::Status> {
        todo!()
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_subscription(
        &self,
        _request: Request<SubscriptionRequest>,
    ) -> std::result::Result<Response<SubscriptionResponse>, tonic::Status> {
        todo!()
    }
}
