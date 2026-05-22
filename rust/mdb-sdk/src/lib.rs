//! Rust SDK for the MarpleDB API.
//!
//! The SDK provides async helpers for checking API health, managing streams,
//! listing datasets, uploading files, waiting for imports, and fetching
//! pre-signed download links.
//!
//! # Quickstart
//!
//! ```no_run
//! use marple_db::{ImportStatus, MarpleDB, PushFileOptions};
//! use serde_json::json;
//! use std::time::Duration;
//!
//! # async fn run() -> marple_db::Result<()> {
//! let db = MarpleDB::new(
//!     "https://db.marpledata.com/api/v1",
//!     "mdb_your_token_here",
//! )?;
//! let stream = db.get_stream("runs").await?;
//! let dataset = db
//!     .push_file(
//!         stream.id,
//!         "run.csv",
//!         PushFileOptions::builder()
//!             .metadata([("source", json!("example"))])
//!             .build(),
//!     )
//!     .await?;
//! let dataset = db
//!     .wait_for_import(stream.id, dataset.id, Duration::from_secs(180))
//!     .await?;
//! assert_eq!(dataset.import_status, ImportStatus::Finished);
//! # Ok(())
//! # }
//! ```
//!
//! # Core Types
//!
//! - [`MarpleDB`] is the API client.
//! - [`PushFileOptions`] configures uploads.
//! - [`ImportStatus`] describes dataset import state.
//! - [`Error`] is the structured SDK error type.
//! - [`ProgressReporter`] receives transfer progress updates.
//!
//! # Errors
//!
//! ```no_run
//! # async fn run(db: marple_db::MarpleDB) -> marple_db::Result<()> {
//! match db.get_stream("runs").await {
//!     Ok(stream) => println!("stream id: {}", stream.id),
//!     Err(marple_db::Error::StreamNotFound { name }) => {
//!         eprintln!("missing stream: {name}");
//!     }
//!     Err(error) => return Err(error),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! This crate is async and does not install a runtime. The examples use Tokio,
//! but callers can use any runtime supported by `reqwest`.

mod client;
mod errors;
mod models;
mod progress;
mod upload;

pub use client::{MarpleDB, MarpleDBBuilder};
pub use errors::{Error, Result};
pub use models::{
    Dataset, HealthResponse, ImportStatus, Metadata, PushFileOptions, PushFileOptionsBuilder,
    Stream, StreamType, UploadModeOverride,
};
pub use progress::{NoopProgress, ProgressReporter};
