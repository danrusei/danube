use crate::broker_server::DanubeServerImpl;
use crate::proto::{
    AckRequest, AckResponse, ConsumerRequest, ConsumerResponse, ReceiveRequest, StreamMessage,
};
use crate::{proto::consumer_service_server::ConsumerService, subscription::SubscriptionOptions};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, trace, Level};

#[tonic::async_trait]
impl ConsumerService for DanubeServerImpl {
    type ReceiveMessagesStream = ReceiverStream<Result<StreamMessage, Status>>;
    // CMD to create a new Consumer
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn subscribe(
        &self,
        request: Request<ConsumerRequest>,
    ) -> Result<Response<ConsumerResponse>, tonic::Status> {
        let req = request.into_inner();

        info!(
            "New Consumer request with name: {} for topic: {} with subscription_type {}",
            req.consumer_name, req.topic_name, req.subscription_type
        );

        // TODO! check if the subscription is authorized to consume from the topic (isTopicOperationAllowed)

        let mut service = self.service.lock().await;

        match service.get_topic(&req.topic_name, None, true).await {
            Ok(_) => trace!("topic_name: {} was found", &req.topic_name),
            Err(status) => {
                info!("Error topic request: {}", status.message());
                return Err(status);
            }
        }

        match service
            .check_if_consumer_exist(&req.consumer_name, &req.subscription, &req.topic_name)
            .await
        {
            Some(consumer_id) => {
                let response = ConsumerResponse {
                    request_id: req.request_id,
                    consumer_id: consumer_id,
                    consumer_name: req.consumer_name,
                };
                return Ok(tonic::Response::new(response));
            }
            None => {
                // if the consumer doesn't exist it attempts to create below
            }
        }

        // check if the topic policies allow the creation of the subscription
        if !service.allow_subscription_creation(&req.topic_name) {
            let status = Status::permission_denied(format!(
                "Not allowed to create the subscription for the topic: {}",
                &req.topic_name
            ));

            return Err(status);
        }

        let subscription_options = SubscriptionOptions {
            subscription_name: req.subscription,
            subscription_type: req.subscription_type,
            consumer_id: None,
            consumer_name: req.consumer_name.clone(),
        };

        let sub_name = subscription_options.subscription_name.clone();

        let consumer_id = match service
            .subscribe(&req.topic_name, subscription_options)
            .await
        {
            Ok(id) => id,
            Err(err) => {
                let status = Status::permission_denied(format!(
                    "Not able to subscribe to the topic {} due to {}",
                    &req.topic_name,
                    err.to_string()
                ));
                return Err(status);
            }
        };

        info!(
            "The Consumer with id: {} for subscription: {}, has been created.",
            consumer_id, sub_name
        );

        let response = ConsumerResponse {
            request_id: req.request_id,
            consumer_id: consumer_id,
            consumer_name: req.consumer_name,
        };

        Ok(tonic::Response::new(response))
    }

    // Stream of messages to Consumer
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn receive_messages(
        &self,
        request: tonic::Request<ReceiveRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReceiveMessagesStream>, tonic::Status> {
        let (tx, rx) = mpsc::channel(4); // Buffer size of 4, adjust as needed
        let (tx_consumer, mut rx_consumer) = mpsc::channel(4);
        let consumer_id = request.into_inner().consumer_id;

        info!(
            "The Consumer with id: {} requested to receive messages",
            consumer_id
        );

        let service = self.service.lock().await;

        let consumer = if let Some(consumer) = service.find_consumer_by_id(consumer_id) {
            consumer
        } else {
            let status = Status::not_found(format!(
                "The consumer with the id {} does not exist",
                consumer_id
            ));
            return Err(status);
        };

        // sends the channel's tx to consumer
        consumer.lock().await.set_tx(tx_consumer);

        //for each consumer spawn a task to send messages
        tokio::spawn(async move {
            loop {
                if let Some(messages) = rx_consumer.recv().await {
                    let request_id = 1;

                    let stream_messages = StreamMessage {
                        request_id: request_id,
                        payload: messages.payload,
                        metadata: messages.metadata,
                    };

                    if tx.send(Ok(stream_messages)).await.is_err() {
                        break;
                    }
                    trace!(
                        "The message with request_id: {} was sent to consumer",
                        request_id
                    );
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // Consumer acknowledge the received message
    async fn ack(
        &self,
        _request: tonic::Request<AckRequest>,
    ) -> std::result::Result<tonic::Response<AckResponse>, tonic::Status> {
        todo!()
    }
}
