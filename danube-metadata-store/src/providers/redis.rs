use crate::{errors::Result, store::MetadataStore, watch::WatchStream};

use async_trait::async_trait;

#[derive(Debug)]
pub struct RedisStore {}

#[async_trait]
impl MetadataStore for RedisStore {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    async fn put(&self, _key: &str, _value: Vec<u8>) -> Result<()> {
        unimplemented!()
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        unimplemented!()
    }

    async fn watch(&self, _prefix: &str) -> Result<WatchStream> {
        unimplemented!()
    }
}
