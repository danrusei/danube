use url::Url;

#[derive(Debug, Clone)]
pub struct RpcConnection {
    id: u64,
    url: Url,
}
