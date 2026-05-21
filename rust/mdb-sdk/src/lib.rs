//! Rust SDK for the MarpleDB API.

mod client;
mod errors;
mod models;
mod progress;
mod upload;

pub use client::{MarpleDB, MarpleDBBuilder};
pub use errors::{Error, Result};
pub use models::{
    Dataset, HealthResponse, ImportStatus, Metadata, PushFileOptions, PushFileOptionsBuilder,
    Stream, UploadModeOverride,
};
pub use progress::{NoopProgress, ProgressReporter};
