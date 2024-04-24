//! Danube-Client
//!
//! Danube-Client -- the Danube stream service client

mod client;
pub use client::DanubeClient;

pub mod errors;

mod producer;
pub use producer::{Producer, ProducerBuilder};
