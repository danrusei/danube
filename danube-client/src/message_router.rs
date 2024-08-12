use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub(crate) struct MessageRouter {
    partitions: usize,
    last_partition: AtomicUsize,
}

impl MessageRouter {
    pub(crate) fn new(partitions: usize) -> Self {
        MessageRouter {
            partitions,
            last_partition: AtomicUsize::new(partitions - 1),
        }
    }
    pub(crate) fn round_robin(&self) -> usize {
        // Atomically get the current value of last_partition
        let last = self.last_partition.load(Ordering::Acquire);

        // Calculate the next partition and update atomically
        let next = (last + 1) % self.partitions;
        self.last_partition.store(next, Ordering::Release);

        next
    }
}
