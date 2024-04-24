use url::Url;

pub(crate) struct LookupResult {
    logical_addr: url::Url,
    physical_addr: url::Url,
}
