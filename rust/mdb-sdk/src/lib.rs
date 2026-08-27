//! Rust SDK for the MarpleDB API.
//!
//! The SDK provides async helpers for checking API health, resolving the
//! current workspace and usage, managing streams, listing datasets and
//! signals, uploading files, waiting for imports, and downloading original
//! uploaded files.
//!
//! # Quickstart
//!
//! ```no_run
//! use marple_db::{ImportStatus, MarpleDB, PushFileOptions, SAAS_URL};
//! use serde_json::json;
//! use std::time::Duration;
//!
//! # async fn run() -> marple_db::Result<()> {
//! let db = MarpleDB::new(SAAS_URL, "mdb_your_token_here")?;
//! let stream = db.get_stream("runs").await?;
//! let dataset = db
//!     .push_file(
//!         stream.id,
//!         "run.csv",
//!         PushFileOptions::default()
//!             .metadata([("source", json!("example"))]),
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
//! - [`SAAS_URL`] is the default Marple DB SaaS API root.
//! - [`VERSION`] is this crate's version string.
//! - [`CurrentWorkspace`] is the resolved workspace from [`MarpleDB::get_current_workspace`].
//! - [`UserInfo`] and [`WorkspaceLicense`] come from `/user/info` and `/workspace/license`.
//! - [`UsageSeries`] is a workspace usage series from [`MarpleDB::get_usage_series`].
//! - [`Settings`] is the workspace settings bag from [`MarpleDB::get_settings`].
//! - [`PushFileOptions`] configures uploads.
//! - [`ImportStatus`] describes dataset import state.
//! - [`Signal`] is dataset signal metadata from [`MarpleDB::get_signals`].
//! - [`Error`] is the structured SDK error type.
//! - [`SourceError`] is the opaque HTTP-client cause of transport/storage failures.
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
//! SDK-built HTTP clients use the same defaults as the Python SDK:
//!
//! The default `rustls-tls-native-roots` feature uses rustls with the OS
//! certificate store, so corporate proxies work.
//!
//! For a custom CA or other TLS settings, pass `reqwest::Client` instances
//! through [`MarpleDBBuilder::client`] and [`MarpleDBBuilder::storage_client`].
//! Those methods follow reqwest's semver; the rest of the SDK does not.
//!
//! This crate is async on Tokio and does not install a runtime. Callers must
//! run it on a Tokio executor (`#[tokio::main]` or equivalent).

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![forbid(unsafe_code)]

#[cfg(not(any(
    feature = "rustls-tls-native-roots",
    feature = "rustls-tls",
    feature = "native-tls"
)))]
compile_error!(
    "marple-db requires the `rustls-tls-native-roots` (default), `rustls-tls`, or `native-tls` Cargo feature"
);

mod client;
mod errors;
mod models;
mod progress;
mod retry;
mod upload;

/// Crate version, matching `CARGO_PKG_VERSION`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default Marple DB SaaS API root.
///
/// Pass this to [`MarpleDB::new`] or [`MarpleDBBuilder::url`] when talking to
/// hosted Marple DB. Self-hosted and VPC deployments should pass their own
/// `/api/v1` URL instead.
pub const SAAS_URL: &str = "https://db.marpledata.com/api/v1";

pub use client::{MarpleDB, MarpleDBBuilder};
pub use errors::{Error, Result, SourceError};
pub use models::{
    CurrentWorkspace, Dataset, HealthResponse, ImportStatus, LicenseLimits, LicensePayload,
    LicenseType, Metadata, PushFileOptions, RealtimeTier, Settings, Signal, StorageQuota,
    StorageStatus, Stream, StreamType, UploadModeOverride, UsageSeries, UsageType, UserInfo,
    WorkspaceLicense, WorkspaceMembership,
};
pub use progress::{NoopProgress, ProgressReporter};
