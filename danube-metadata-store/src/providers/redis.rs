use crate::{
    errors::Result,
    store::{MetaOptions, MetadataStore},
    watch::WatchStream,
};

use async_trait::async_trait;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct RedisStore {}

#[async_trait]
impl MetadataStore for RedisStore {
    async fn get(&self, _key: &str, _get_options: MetaOptions) -> Result<Option<Value>> {
        unimplemented!()
    }

    async fn get_childrens(&self, _path: &str) -> Result<Vec<String>> {
        unimplemented!()
    }

    async fn put(&self, _key: &str, _value: Value, _put_options: MetaOptions) -> Result<()> {
        unimplemented!()
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        unimplemented!()
    }

    async fn watch(&self, _prefix: &str) -> Result<WatchStream> {
        unimplemented!()
    }
}
