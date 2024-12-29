use etcd_client::{EventType, WatchStream as EtcdWatchStream};
use futures::stream::Stream;
use futures::StreamExt;
use std::pin::Pin;

use crate::errors::{MetadataError, Result};

pub enum WatchEvent {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        mod_revision: Option<i64>,
        version: Option<i64>,
    },
    Delete {
        key: Vec<u8>,
        mod_revision: Option<i64>,
        version: Option<i64>,
    },
}

pub struct WatchStream {
    inner: Pin<Box<dyn Stream<Item = Result<WatchEvent>> + Send>>,
}

impl WatchStream {
    pub(crate) fn from_etcd(stream: EtcdWatchStream) -> Self {
        let stream = stream.flat_map(|result| {
            futures::stream::iter(
                result
                    .map_err(MetadataError::from)
                    .and_then(|watch_response| {
                        Ok(watch_response
                            .events()
                            .iter()
                            .map(|event| {
                                let key_value = event.kv().unwrap();
                                match event.event_type() {
                                    EventType::Put => Ok(WatchEvent::Put {
                                        key: key_value.key().to_vec(),
                                        value: key_value.value().to_vec(),
                                        mod_revision: Some(key_value.mod_revision()),
                                        version: Some(key_value.version()),
                                    }),
                                    EventType::Delete => Ok(WatchEvent::Delete {
                                        key: key_value.key().to_vec(),
                                        mod_revision: Some(key_value.mod_revision()),
                                        version: Some(key_value.version()),
                                    }),
                                }
                            })
                            .collect::<Vec<_>>())
                    })
                    .into_iter()
                    .flatten(),
            )
        });

        Self {
            inner: Box::pin(stream),
        }
    }
}

// PREVIOUS IMPLEMENTATION KEEP HERE FOR REFERENCE

// #[derive(Debug)]
// #[allow(dead_code)]
// pub(crate) struct ETCDWatchEvent {
//     pub(crate) key: String,
//     pub(crate) value: Option<Vec<u8>>,
//     pub(crate) mod_revision: i64,
//     pub(crate) version: i64,
//     pub(crate) event_type: EventType,
// }

// pub(crate) async fn etcd_watch_prefixes(
//     client: Client,
//     prefixes: Vec<String>,
//     sender: mpsc::Sender<ETCDWatchEvent>,
// ) -> Result<()> {
//     debug!(
//         "A new watch request has beed received for prefixes: {:?}",
//         prefixes
//     );
//     for prefix in prefixes {
//         let client_clone = client.clone();
//         let sender_clone = sender.clone();
//         tokio::spawn(async move {
//             if let Err(e) = watch_prefix(client_clone, &prefix, sender_clone).await {
//                 warn!("Error watching prefix {}: {}", prefix, e);
//             }
//         });
//     }

//     // another solution with futures::stream::FuturesUnordered
//     // for relative low number of watches both works fine

//     // let mut tasks = FuturesUnordered::new();
//     //
//     // for prefix in prefixes {
//     //     let client_clone = client.clone();
//     //     let sender_clone = sender.clone();
//     //     tasks.push(watch_prefix(client_clone, prefix, sender_clone));
//     // }
//     // while let Some(result) = tasks.next().await {
//     //     if let Err(e) = result {
//     //         warn!("Error in watching prefixes: {}", e);
//     //     }
//     // }

//     Ok(())
// }

// async fn watch_prefix(
//     mut client: Client,
//     prefix: &str,
//     sender: mpsc::Sender<ETCDWatchEvent>,
// ) -> Result<()> {
//     let (_watcher, mut watch_stream) = client
//         .watch(prefix, Some(WatchOptions::new().with_prefix()))
//         .await?;

//     while let Some(response) = watch_stream.next().await {
//         match response {
//             Ok(watch_response) => {
//                 for event in watch_response.events() {
//                     if let Some(key_value) = event.kv() {
//                         let watch_event = ETCDWatchEvent {
//                             key: key_value.key_str().unwrap().to_owned(),
//                             value: Some(key_value.value().to_vec()),
//                             mod_revision: key_value.mod_revision(),
//                             version: key_value.version(),
//                             event_type: event.event_type(),
//                         };
//                         if let Err(e) = sender.send(watch_event).await {
//                             warn!("Failed to send watch event: {}", e);
//                         }
//                     }
//                 }
//             }
//             Err(e) => {
//                 warn!("Watch error: {}", e);
//                 // either break or continue here ?
//                 break;
//             }
//         }
//     }
//     Ok(())
// }
