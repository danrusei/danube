use crate::proto::{danube_client, ProducerAccessMode, ProducerRequest, Schema};
use crate::{errors::Result, DanubeClient};

pub struct Producer {
    danube: DanubeClient,
    topic: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProducerBuilder {
    danube: DanubeClient,
    topic: Option<String>,
    name: Option<String>,
}

impl ProducerBuilder {
    pub fn new(client: &DanubeClient) -> Self {
        ProducerBuilder {
            danube: client.clone(),
            topic: None,
            name: None,
        }
    }

    /// sets the producer's topic
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// sets the producer's name
    pub fn with_name<S: Into<String>>(mut self, name: S) -> Self {
        self.name = Some(name.into());
        self
    }

    pub async fn create(self) -> Result<Producer> {
        let req = ProducerRequest {
            request_id: 1,
            producer_id: 2,
            producer_name: "hello_producer".to_string(),
            topic: "hello_topic".to_string(),
            schema: Some(Schema {
                name: "schema_name".to_string(),
                schema_data: "1".as_bytes().to_vec(),
                type_schema: 0,
            }),
            producer_access_mode: ProducerAccessMode::Shared.into(),
        };

        let request = tonic::Request::new(req);

        let grpc_cnx = self
            .danube
            .cnx_manager
            .get_connection(&self.danube.uri, &self.danube.uri)
            .await?;

        let mut client = danube_client::DanubeClient::new(grpc_cnx.grpc_cnx.clone());
        let response = client.create_producer(request).await?;

        println!("Response: {:?}", response.get_ref().request_id);

        Ok(Producer {
            danube: self.danube,
            topic: self.topic,
            name: self.name,
        })
    }
}
