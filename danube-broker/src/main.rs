use proto::danube_server::{Danube, DanubeServer};
use tonic::transport::Server;
use tonic::{Request, Response};

mod proto {
    include!("../../proto/danube.rs");
}

#[derive(Debug, Default)]
struct DanubeService {}

#[tonic::async_trait]
impl Danube for DanubeService {
    async fn create_producer(
        &self,
        request: Request<proto::ProducerRequest>,
    ) -> Result<Response<proto::ProducerResponse>, tonic::Status> {
        let req = request.get_ref();

        println!(
            "{} {} {} {}",
            req.request_id, req.producer_id, req.producer_name, req.topic,
        );

        let response = proto::ProducerResponse {
            request_id: req.request_id,
        };

        Ok(tonic::Response::new(response))
    }
    async fn subscribe(
        &self,
        request: Request<proto::ConsumerRequest>,
    ) -> Result<Response<proto::ConsumerResponse>, tonic::Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:6650".parse()?;

    let danube = DanubeService::default();

    Server::builder()
        .add_service(DanubeServer::new(danube))
        .serve(addr)
        .await?;

    Ok(())
}
