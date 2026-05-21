use reqwest::{Method, StatusCode};
use std::num::TryFromIntError;
use thiserror::Error;

/// Error type returned by the MarpleDB SDK.
///
/// `Transport` means no usable HTTP response was received, `Api` means the
/// MarpleDB API returned a non-success status, and `Storage` covers direct
/// pre-signed storage uploads/downloads.
///
/// ```
/// # fn handle(error: marple_db::Error) {
/// match error {
///     marple_db::Error::Api { status, body, .. } => {
///         eprintln!("API returned {status}: {body}");
///     }
///     error if error.status().is_some() => {
///         eprintln!("HTTP-like error: {:?}", error.status());
///     }
///     error => eprintln!("{error}"),
/// }
/// # }
/// ```
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    /// The SDK was configured with invalid input.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// Building an HTTP header failed.
    #[error("invalid HTTP header value")]
    Header(#[from] reqwest::header::InvalidHeaderValue),

    /// A request failed before receiving an API response.
    #[error("HTTP transport error on {method} {endpoint}")]
    Transport {
        /// HTTP method used for the request.
        method: Method,
        /// API endpoint or URL being requested.
        endpoint: String,
        /// Underlying reqwest error.
        #[source]
        source: reqwest::Error,
    },

    /// The MarpleDB API returned a non-success HTTP status.
    #[error("MarpleDB API returned {status} on {method} {endpoint}: {body}")]
    Api {
        /// HTTP method used for the request.
        method: Method,
        /// API endpoint being requested.
        endpoint: String,
        /// Response status code.
        status: StatusCode,
        /// Response body text.
        body: String,
    },

    /// Direct storage upload or download failed.
    #[error("storage transfer failed: {context}")]
    Storage {
        /// Human-readable storage operation context.
        context: String,
        /// HTTP status code when the storage service responded with one.
        status: Option<StatusCode>,
        /// Response body text when available.
        body: Option<String>,
        /// Underlying reqwest error when the request failed before a response.
        #[source]
        source: Option<reqwest::Error>,
    },

    /// A stream with the requested name was not found.
    #[error("stream {name:?} not found")]
    StreamNotFound {
        /// Requested stream name.
        name: String,
    },

    /// A stream with the requested id was not found.
    #[error("stream {id} not found")]
    StreamIdNotFound {
        /// Requested stream id.
        id: i32,
    },

    /// The dataset has no original-file backup available for download.
    #[error("dataset {id} has no backup available")]
    NoBackup {
        /// Dataset id.
        id: i32,
    },

    /// Import polling reached its timeout before a terminal status.
    #[error("ingestion timed out after {timeout_secs}s, last status: {last_status}")]
    ImportTimeout {
        /// Timeout in seconds.
        timeout_secs: u64,
        /// Last observed import status.
        last_status: String,
    },

    /// Import polling reached a failed terminal status.
    #[error("ingestion failed for dataset {id}: {message}")]
    ImportFailed {
        /// Dataset id.
        id: i32,
        /// Failure message from the API, if present.
        message: String,
    },

    /// The API returned a response that does not match the SDK protocol.
    #[error("invalid server response: {0}")]
    Protocol(String),

    /// Local filesystem I/O failed.
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    /// URL parsing failed.
    #[error("URL parse error")]
    Url(#[from] url::ParseError),

    /// JSON serialization or deserialization failed.
    #[error("JSON error")]
    Json(#[from] serde_json::Error),

    /// Integer conversion failed.
    #[error("integer conversion failed")]
    IntegerConversion(#[from] TryFromIntError),
}

impl Error {
    /// Returns the HTTP status for API or storage responses that provided one.
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::Api { status, .. } => Some(*status),
            Self::Storage { status, .. } => *status,
            _ => None,
        }
    }
}

/// Result type returned by the MarpleDB SDK.
pub type Result<T> = std::result::Result<T, Error>;
