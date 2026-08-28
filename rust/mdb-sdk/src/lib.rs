//! Async Rust client for the MarpleDB API.
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
//!         PushFileOptions::default().metadata([("source", json!("example"))]),
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
//! # Notes
//! - Requires a Tokio runtime (`#[tokio::main]` or equivalent).
//! - Match [`Error`] for API failures.
//! - Default TLS uses the OS certificate store (`rustls-tls-native-roots`).
//! - For a custom CA, pass `reqwest::Client`s to
//!   [`MarpleDBBuilder::client`] and [`MarpleDBBuilder::storage_client`].

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
    CurrentWorkspace, Dataset, DatasetStatus, HealthResponse, ImportStatus, LicenseLimits,
    LicensePayload, LicenseType, Metadata, PushFileOptions, RealtimeTier, Settings, Signal,
    StorageQuota, StorageStatus, Stream, StreamType, UploadModeOverride, UsageSeries, UsageType,
    UserInfo, WorkspaceLicense, WorkspaceMembership,
};
pub use progress::{NoopProgress, ProgressReporter};
pub use upload::UploadSession;
