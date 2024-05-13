use crate::metadata_store::{EtcdMetadataStore, MemoryMetadataStore, MetadataStorage};
use crate::proto::danube_server::{Danube, DanubeServer};
use crate::proto::{ConsumerRequest, ConsumerResponse, ProducerRequest, ProducerResponse};
use crate::resources::DanubeResources;
use crate::service_configuration::ServiceConfiguration;
use crate::storage;

use tonic::transport::Server;
use tonic::{Request, Response};

#[derive(Debug)]
pub(crate) struct DanubeService {
    config: ServiceConfiguration,
    resources: DanubeResources,
}

impl DanubeService {
    pub(crate) fn new(broker_config: ServiceConfiguration) -> Self {
        DanubeService {
            config: broker_config,
            resources: DanubeResources::new(),
        }
    }

    pub(crate) async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        let socket_addr = self.config.broker_addr.clone();

        let metadata_store: MetadataStorage = if let Some(etcd_addr) = self.config.etcd_addr.clone()
        {
            MetadataStorage::EtcdStore(EtcdMetadataStore::new(etcd_addr).await?)
        } else {
            MetadataStorage::MemoryStore(MemoryMetadataStore::new().await?)
        };

        let storage = storage::Storage::new();

        //TODO! initialize the broker

        //TODO! start the broker

        //TODO! here we may want to start the MetadataEventSynchronizer & CoordinationService

        // to be moved to Broker start !!
        Server::builder()
            .add_service(DanubeServer::new(self))
            .serve(socket_addr)
            .await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl Danube for DanubeService {
    async fn create_producer(
        &self,
        request: Request<ProducerRequest>,
    ) -> Result<Response<ProducerResponse>, tonic::Status> {
        let req = request.get_ref();

        println!(
            "{} {} {} {}",
            req.request_id, req.producer_id, req.producer_name, req.topic,
        );

        let response = ProducerResponse {
            request_id: req.request_id,
        };

        Ok(tonic::Response::new(response))
    }
    async fn subscribe(
        &self,
        _request: Request<ConsumerRequest>,
    ) -> Result<Response<ConsumerResponse>, tonic::Status> {
        todo!()
    }
}
