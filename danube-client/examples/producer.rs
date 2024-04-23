use anyhow::Result;
use danube_client::DanubeClient;

#[tokio::main]
async fn main() -> Result<()> {
    let client = DanubeClient::builder()
        .service_url("http://[::1]:6650")
        .build();
    client.connect().await?;

    Ok(())
}
