use crate::errors::Result;
use proto::danube_client;

pub mod proto {
    include!("../../proto/danube.rs");
}

#[derive(Debug, Default)]
pub struct DanubeClient {}

impl DanubeClient {
    pub fn new() -> Self {
        DanubeClient::default()
    }
    pub async fn connect(&self) -> Result<()> {
        let url = "http://[::1]:6650";
        let mut client = danube_client::DanubeClient::connect(url).await?;

        let req = proto::ProducerRequest {
            request_id: 1,
            producer_id: 2,
            producer_name: "hello_producer".to_string(),
            topic: "hello_topic".to_string(),
            schema: Some(proto::Schema {
                name: "schema_name".to_string(),
                schema_data: "1".as_bytes().to_vec(),
                type_schema: 0,
            }),
        };

        let request = tonic::Request::new(req);
        let response = client.create_producer(request).await?;

        println!("Response: {:?}", response.get_ref().request_id);

        Ok(())
    }
}
