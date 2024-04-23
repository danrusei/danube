use anyhow::Result;
use danube_client::DanubeClient;

#[tokio::main]
async fn main() -> Result<()> {
    let client = DanubeClient::new();
    client.connect().await?;

    Ok(())
}
