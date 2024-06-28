use anyhow::{anyhow, Result};
use etcd_client::{Client, PutOptions as EtcdPutOptions};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info};

use crate::metadata_store::{MetaOptions, MetadataStorage, MetadataStore};
use crate::resources::BASE_REGISTER_PATH;
use crate::utils::join_path;

pub(crate) async fn register_broker(
    mut store: MetadataStorage,
    broker_id: &str,
    broker_addr: &str,
    ttl: i64,
) -> Result<()> {
    let mut client = if let Some(client) = store.get_client() {
        client
    } else {
        return Err(anyhow!("unable to get the etcd_client"));
    };

    // Create a lease with a TTL (time to live)
    let lease = client.lease_grant(ttl, None).await?;
    let lease_id = lease.id();
    let put_opts = EtcdPutOptions::new().with_lease(lease_id);

    let path = join_path(&[BASE_REGISTER_PATH, broker_id]);
    let broker_uri = format!("http://{}", broker_addr);
    let payload = serde_json::Value::String(broker_uri);

    // Register the broker with a key associated with the lease
    match store
        .put(&path, payload, MetaOptions::EtcdPut(put_opts))
        .await
    {
        Ok(_) => {
            info!("Broker {} registered in the cluster", broker_id);
            keep_alive_lease(client, lease_id, ttl).await?;
        }
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

async fn keep_alive_lease(mut client: Client, lease_id: i64, ttl: i64) -> Result<()> {
    let (mut keeper, mut stream) = client.lease_keep_alive(lease_id).await?;
    // Periodically send keep-alive requests to etcd
    tokio::spawn(async move {
        loop {
            // Attempt to send a keep-alive request
            match keeper.keep_alive().await {
                Ok(_) => debug!(
                    "Broker Register, keep-alive request sent for lease {}",
                    lease_id
                ),
                Err(e) => {
                    error!(
                        "Broker Register, failed to send keep-alive request for lease {}: {}",
                        lease_id, e
                    );
                    break;
                }
            }

            // Check for responses from etcd to confirm the lease is still alive
            match stream.message().await {
                Ok(Some(_response)) => {
                    debug!(
                        "Broker Register, received keep-alive response for lease {}",
                        lease_id
                    );
                }
                Ok(None) => {
                    error!(
                        "Broker Register, keep-alive response stream ended unexpectedly for lease {}",
                        lease_id
                    );
                    break;
                }
                Err(e) => {
                    error!(
                        "Broker Register, failed to receive keep-alive response for lease {}: {}",
                        lease_id, e
                    );
                    break;
                }
            }

            // Sleep for a period shorter than the lease TTL to ensure continuous renewal
            sleep(Duration::from_secs(ttl as u64 / 2)).await;
        }
    });

    Ok(())
}
