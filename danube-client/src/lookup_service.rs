use crate::{
    connection_manager::ConnectionManager,
    errors::{decode_error_details, error_type_from_i32, DanubeError, Result},
};

use crate::proto::{
    discovery_client::DiscoveryClient, topic_lookup_response::LookupType, TopicLookupRequest,
    TopicLookupResponse,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tonic::transport::Uri;
use tonic::{Response, Status};
use url::Url;

#[derive(Debug, Default)]
pub struct LookupResult {
    response_type: i32,
    addr: Uri,
}

#[derive(Debug, Clone)]
pub(crate) struct LookupService {
    cnx_manager: Arc<ConnectionManager>,
    // unique identifier for every request sent by LookupService
    request_id: Arc<AtomicU64>,
}

impl LookupService {
    pub fn new(cnx_manager: Arc<ConnectionManager>) -> Self {
        LookupService {
            cnx_manager,
            request_id: Arc::new(AtomicU64::new(0)),
        }
    }
    pub async fn lookup_topic(&self, addr: &Uri, topic: impl Into<String>) -> Result<LookupResult> {
        let grpc_cnx = self.cnx_manager.get_connection(addr, addr).await?;

        let mut client = DiscoveryClient::new(grpc_cnx.grpc_cnx.clone());

        let lookup_request = TopicLookupRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            topic: topic.into(),
        };

        let request = tonic::Request::new(lookup_request);

        let response: std::result::Result<Response<TopicLookupResponse>, Status> =
            client.topic_lookup(request).await;

        let mut lookup_result = LookupResult::default();

        match response {
            Ok(resp) => {
                let lookup_resp = resp.into_inner();
                let addr_to_uri = lookup_resp.broker_service_url.parse::<Uri>()?;

                lookup_result.response_type = lookup_resp.response_type;
                lookup_result.addr = addr_to_uri;
            }
            // maybe some checks on the status, if anything can be handled by server
            Err(status) => {
                let decoded_message = decode_error_details(&status);
                return Err(DanubeError::FromStatus(status, decoded_message));
            }
        };

        Ok(lookup_result)
    }

    // for SERVICE_NOT_READY error received from broker retry the topic_lookup request
    // as the topic may be in process to be assigned to a broker in cluster
    pub(crate) async fn handle_lookup_and_retries(
        &self,
        addr: &Uri,
        status: &Status,
        topic: &str,
    ) -> Result<Uri> {
        if let Some(error_details) = decode_error_details(&status) {
            if error_details.error_type == 3 {
                //ServiceNotReady
                match self.lookup_topic(&addr, topic).await {
                    Ok(lookup_result) => match lookup_type_from_i32(lookup_result.response_type) {
                        Some(LookupType::Redirect) => Ok(lookup_result.addr),
                        Some(LookupType::Connect) => Ok(addr.to_owned()),
                        Some(LookupType::Failed) => Err(DanubeError::Unrecoverable(format!(
                            "Lookup failed for topic: {}",
                            topic
                        ))),
                        None => {
                            todo!()
                        }
                    },
                    Err(err) => Err(DanubeError::Unrecoverable(format!(
                        "Lookup failed for topic: {}, with error: {}",
                        topic, err
                    ))),
                }
            } else {
                //  let decoded_message = decode_error_details(&status);
                Err(DanubeError::FromStatus(status.clone(), Some(error_details)))
            }
        } else {
            Err(DanubeError::FromStatus(status.clone(), None))
        }
    }
}

// A helper function to convert i32 to LookupType.
fn lookup_type_from_i32(value: i32) -> Option<LookupType> {
    match value {
        0 => Some(LookupType::Redirect),
        1 => Some(LookupType::Connect),
        2 => Some(LookupType::Failed),
        _ => None,
    }
}
