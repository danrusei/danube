use crate::admin::DanubeAdminImpl;
use crate::admin_proto::{
    topic_admin_server::TopicAdmin, NamespaceRequest, NewTopicRequest, SubscriptionListResponse,
    SubscriptionRequest, SubscriptionResponse, TopicListResponse, TopicRequest, TopicResponse,
};
use crate::proto::TopicDeliveryStrategy;
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
        request: Request<NewTopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!("Admin: creates a non-partitioned topic: {}", req.name);

        let mut schema_type = match SchemaType::from_str(&req.schema_type) {
            Some(schema_type) => schema_type,
            None => {
                let status = Status::not_found(format!(
                    "Invalid schema_type, allowed values: Bytes, String, Int64, Json "
                ));
                return Err(status);
            }
        };

        if schema_type == SchemaType::Json(String::new()) {
            schema_type = SchemaType::Json(req.schema_data);
        }

        let ret_strategy = TopicDeliveryStrategy {
            strategy: req.retention_strategy,
            retention_period: 3600,
            segment_size: 50,
        };

        let mut service = self.broker_service.lock().await;

        let schema = Schema::new(format!("{}_schema", req.name), schema_type);

        let success = match service
            .create_topic_cluster(&req.name, Some(ret_strategy), Some(schema.into()))
            .await
        {
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
    async fn unsubscribe(
        &self,
        request: Request<SubscriptionRequest>,
    ) -> std::result::Result<Response<SubscriptionResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!(
            "Admin: Unsubscribe subscription {} from topic: {}",
            req.subscription,
            req.topic
        );

        let mut service = self.broker_service.lock().await;

        let success = match service.unsubscribe(&req.subscription, &req.topic).await {
            Ok(()) => true,
            Err(err) => {
                let status = Status::not_found(format!(
                    "Unable to unsubscribe the subscription {} due to error: {}",
                    req.subscription, err
                ));
                return Err(status);
            }
        };

        let response = SubscriptionResponse { success };
        Ok(tonic::Response::new(response))
    }
}
