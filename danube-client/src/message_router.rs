/// Represents a Producer
#[derive(Debug)]
pub(crate) struct MessageRouter {}

impl MessageRouter {
    pub(crate) fn new() -> Self {
        MessageRouter {}
    }
    pub(crate) fn get_partition(&self) -> usize {
        return 1;
    }
}
