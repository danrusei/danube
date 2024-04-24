use crate::{connection_manager::ConnectionManager, errors::Result};
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
pub struct LookupResult {
    logical_addr: url::Url,
    physical_addr: url::Url,
}

#[derive(Debug)]
pub(crate) struct LookupService {
    cnx_manager: Arc<ConnectionManager>,
}

impl LookupService {
    pub fn new(cnx_manager: Arc<ConnectionManager>) -> Self {
        LookupService { cnx_manager }
    }
    pub async fn lookup_topic(&self, topic: impl Into<String>) -> Result<LookupResult> {
        todo!()
    }
}
