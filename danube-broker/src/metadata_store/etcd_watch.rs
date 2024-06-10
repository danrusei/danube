use anyhow::Result;
use etcd_client::{Client, EventType, WatchOptions};
use futures::StreamExt;
use tokio::sync::mpsc;
use tracing::warn;

// Represent an event of interest
#[derive(Debug)]
pub(crate) struct ETCDWatchEvent {
    pub(crate) key: String,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) mod_revision: i64,
    pub(crate) version: i64,
    pub(crate) event_type: EventType,
}

pub(crate) async fn etcd_watch_prefixes(
    mut client: Client,
    prefixes: Vec<&str>,
    sender: mpsc::Sender<ETCDWatchEvent>,
) -> Result<()> {
    for prefix in prefixes {
        let (watcher, mut watch_stream) = client
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
                            sender.send(watch_event).await.unwrap();
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
    }
    Ok(())
}
