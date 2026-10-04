//! Subsegment stream engine — library root.
//!
//! Production-oriented backend that ingests live broadcast streams,
//! normalizes/transcodes them and serves authenticated HTTP listeners
//! through shared pipelines.

pub mod api;
pub mod auth;
pub mod config;
pub mod error;
pub mod ingest;
pub mod metadata;
pub mod pipeline;
pub mod security;
pub mod telemetry;
pub mod transcoder;
pub mod types;

pub use error::EngineError;
pub use types::{Codec, Quality, SourceType, StreamMetadata, StreamSpec};
