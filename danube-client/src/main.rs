use proto::danube_client::DanubeClient;
use std::error::Error;

pub mod proto {
    include!("../../proto/danube.rs");
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = "http://[::1]:6650";
    let mut client = DanubeClient::connect(url).await?;

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
