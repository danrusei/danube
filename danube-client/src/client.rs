use tonic::transport::Uri;

use crate::{
    connection_manager::{ConnectionManager, ConnectionOptions},
    consumer::ConsumerBuilder,
    errors::Result,
    health_check::HealthCheckService,
    lookup_service::{LookupResult, LookupService},
    producer::ProducerBuilder,
    schema::Schema,
    schema_service::SchemaService,
};

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DanubeClient {
    pub(crate) uri: Uri,
    pub(crate) cnx_manager: Arc<ConnectionManager>,
    pub(crate) lookup_service: LookupService,
    pub(crate) schema_service: SchemaService,
    pub(crate) health_check_service: HealthCheckService,
}

impl DanubeClient {
    fn new_client(builder: DanubeClientBuilder, uri: Uri) -> Self {
        let cnx_manager = ConnectionManager::new(builder.connection_options);
        let cnx_manager = Arc::new(cnx_manager);

        let lookup_service = LookupService::new(cnx_manager.clone());

        let schema_service = SchemaService::new(cnx_manager.clone());

        let health_check_service = HealthCheckService::new(cnx_manager.clone());

        DanubeClient {
            uri: uri,
            cnx_manager,
            lookup_service,
            schema_service,
            health_check_service,
        }
    }
    //creates a Client Builder
    pub fn builder() -> DanubeClientBuilder {
        DanubeClientBuilder::default()
    }

    /// creates a Producer Builder
    pub fn new_producer(&self) -> ProducerBuilder {
        ProducerBuilder::new(self)
    }

    /// creates a Consumer Builder
    pub fn new_consumer(&self) -> ConsumerBuilder {
        ConsumerBuilder::new(self)
    }

    /// gets the address of a broker handling the topic
    pub async fn lookup_topic(&self, addr: &Uri, topic: impl Into<String>) -> Result<LookupResult> {
        self.lookup_service.lookup_topic(addr, topic).await
    }

    /// gets the schema for the requested topic
    pub async fn get_schema(&self, topic: impl Into<String>) -> Result<Schema> {
        self.schema_service.get_schema(&self.uri, topic).await
    }
}

#[derive(Debug, Clone, Default)]
pub struct DanubeClientBuilder {
    uri: String,
    connection_options: ConnectionOptions,
}

impl DanubeClientBuilder {
    pub fn service_url(mut self, url: impl Into<String>) -> Self {
        self.uri = url.into();

        self
    }
    pub fn with_connection_options(mut self, connection_options: ConnectionOptions) -> Self {
        self.connection_options = connection_options;

        self
    }
    pub fn build(self) -> Result<DanubeClient> {
        let uri = self.uri.parse::<Uri>()?;
        Ok(DanubeClient::new_client(self, uri))
    }
}
