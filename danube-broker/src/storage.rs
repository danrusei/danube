use bytes::Bytes;

#[derive(Debug)]
pub(crate) struct Storage {
    data: Bytes,
}

impl Storage {
    pub(crate) fn new() -> Self {
        Storage { data: Bytes::new() }
    }

    pub(crate) fn set_data(&mut self, bytes: Bytes) {
        self.data = bytes;
    }

    pub(crate) fn get_data(&self) -> &Bytes {
        &self.data
    }
}

// TODO! need create the abstraction mechanism and offload to other databases, or drives, S3 etc
// maybe a generic Storage trait
