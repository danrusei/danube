use crate::admin::DanubeAdminImpl;
use crate::admin_proto::{
    topic_admin_server::TopicAdmin, NamespaceRequest, PartitionedTopicRequest,
    SubscriptionListResponse, SubscriptionRequest, SubscriptionResponse, TopicListResponse,
    TopicRequest, TopicResponse,
};
use crate::schema::{Schema, SchemaType};

use tonic::{Request, Response, Status};
use tracing::{trace, Level};

#[tonic::async_trait]
impl TopicAdmin for DanubeAdminImpl {
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_topics(
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
    async fn create_topic(
        &self,
        request: Request<TopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!("Admin: creates a non-partitioned topic: {}", req.name);

        let mut service = self.broker_service.lock().await;

        let schema = Schema::new(format!("{}_schema", req.name), SchemaType::String);

        let success = match service.post_new_topic(&req.name, schema.into(), None).await {
            Ok(()) => true,
            Err(err) => {
                let status = Status::not_found(format!(
                    "Unable to create the topic {} due to {}",
                    req.name, err
                ));
                return Err(status);
            }
        };

        let response = TopicResponse { success };
        Ok(tonic::Response::new(response))
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
        request: Request<TopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!("Admin: delete the topic: {}", req.name);

        let mut service = self.broker_service.lock().await;

        let success = match service.post_delete_topic(&req.name).await {
            Ok(()) => true,
            Err(err) => {
                let status = Status::not_found(format!(
                    "Unable to delete the topic {} due to {}",
                    req.name, err
                ));
                return Err(status);
            }
        };

        let response = TopicResponse { success };
        Ok(tonic::Response::new(response))
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
        request: Request<TopicRequest>,
    ) -> std::result::Result<Response<SubscriptionListResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!(
            "Admin: get the list of subscriptions on the topic: {}",
            req.name
        );

        let subscriptions = self
            .resources
            .topic
            .get_subscription_for_topic(&req.name)
            .await;

        let response = SubscriptionListResponse { subscriptions };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_subscription(
        &self,
        _request: Request<SubscriptionRequest>,
    ) -> std::result::Result<Response<SubscriptionResponse>, tonic::Status> {
        todo!()
    }
}
