//! Danube-Client
//!
//! Danube-Client -- the Danube stream service client

pub mod proto {
    include!("../../proto/danube.rs");
}

mod client;
pub use client::DanubeClient;

pub mod errors;

mod producer;
pub use producer::{Producer, ProducerBuilder, ProducerOptions};

mod schema;
pub use schema::SchemaType;

mod message;

mod lookup_service;

mod connection_manager;

mod rpc_connection;
