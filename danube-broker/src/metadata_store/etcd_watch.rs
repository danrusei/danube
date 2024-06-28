use anyhow::Result;
use etcd_client::{Client, EventType, WatchOptions};
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::{debug, warn};

// Represent an event of interest
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ETCDWatchEvent {
    pub(crate) key: String,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) mod_revision: i64,
    pub(crate) version: i64,
    pub(crate) event_type: EventType,
}

pub(crate) async fn etcd_watch_prefixes(
    client: Client,
    prefixes: Vec<String>,
    sender: mpsc::Sender<ETCDWatchEvent>,
) -> Result<()> {
    debug!(
        "A new watch request has beed received for prefixes: {:?}",
        prefixes
    );
    for prefix in prefixes {
        let client_clone = client.clone();
        let sender_clone = sender.clone();
        tokio::spawn(async move {
            if let Err(e) = watch_prefix(client_clone, &prefix, sender_clone).await {
                warn!("Error watching prefix {}: {}", prefix, e);
            }
        });
    }

    // another solution with futures::stream::FuturesUnordered
    // for relative low number of watches both works fine

    // let mut tasks = FuturesUnordered::new();
    //
    // for prefix in prefixes {
    //     let client_clone = client.clone();
    //     let sender_clone = sender.clone();
    //     tasks.push(watch_prefix(client_clone, prefix, sender_clone));
    // }
    // while let Some(result) = tasks.next().await {
    //     if let Err(e) = result {
    //         warn!("Error in watching prefixes: {}", e);
    //     }
    // }

    Ok(())
}

async fn watch_prefix(
    mut client: Client,
    prefix: &str,
    sender: mpsc::Sender<ETCDWatchEvent>,
) -> Result<()> {
    let (_watcher, mut watch_stream) = client
        .watch(prefix, Some(WatchOptions::new().with_prefix()))
        .await?;

    while let Some(response) = watch_stream.next().await {
        match response {
            Ok(watch_response) => {
                for event in watch_response.events() {
                    if let Some(key_value) = event.kv() {
                        let watch_event = ETCDWatchEvent {
                            key: key_value.key_str().unwrap().to_owned(),
                            value: Some(key_value.value().to_vec()),
                            mod_revision: key_value.mod_revision(),
                            version: key_value.version(),
                            event_type: event.event_type(),
                        };
                        if let Err(e) = sender.send(watch_event).await {
                            warn!("Failed to send watch event: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Watch error: {}", e);
                // either break or continue here ?
                break;
            }
        }
    }
    Ok(())
}
