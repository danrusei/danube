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

mod consumer;
pub use consumer::{Consumer, ConsumerBuilder, ConsumerOptions, SubType};

mod schema;
pub use schema::{Schema, SchemaType};

mod schema_service;

mod message;

mod lookup_service;

mod connection_manager;

mod rpc_connection;
