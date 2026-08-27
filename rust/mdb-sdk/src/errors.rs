use std::error::Error as StdError;
use std::fmt;
use std::num::TryFromIntError;
use thiserror::Error;

/// Opaque cause of an HTTP transport or storage failure.
///
/// The SDK keeps the underlying client error out of the public type so
/// `reqwest` version bumps are not breaking changes. Use [`std::fmt::Display`]
/// or [`StdError::source`] for diagnostics.
pub struct SourceError {
    inner: Box<dyn StdError + Send + Sync>,
}

impl SourceError {
    pub(crate) fn new<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            inner: Box::new(error),
        }
    }
}

impl fmt::Debug for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner.to_string(), f)
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl StdError for SourceError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&*self.inner)
    }
}

/// Error type returned by the MarpleDB SDK.
///
/// `Transport` means no usable HTTP response was received, `Api` means the
/// MarpleDB API returned a non-success status, and `Storage` covers direct
/// pre-signed storage uploads/downloads.
///
/// HTTP methods are strings such as `GET`. Status codes are raw `u16` values
/// (`404`, `503`, …).
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

    /// A request failed before receiving an API response.
    #[error("HTTP transport error on {method} {endpoint}")]
    Transport {
        /// HTTP method used for the request, such as `GET`.
        method: String,
        /// API endpoint or URL being requested.
        endpoint: String,
        /// Underlying HTTP client error.
        #[source]
        source: SourceError,
    },

    /// The MarpleDB API returned a non-success HTTP status.
    #[error("MarpleDB API returned {status} on {method} {endpoint}: {body}")]
    Api {
        /// HTTP method used for the request, such as `GET`.
        method: String,
        /// API endpoint being requested.
        endpoint: String,
        /// Response status code.
        status: u16,
        /// Response body text.
        body: String,
    },

    /// Direct storage upload or download failed.
    #[error("storage transfer failed: {context}")]
    Storage {
        /// Human-readable storage operation context.
        context: String,
        /// HTTP status code when the storage service responded with one.
        status: Option<u16>,
        /// Response body text when available.
        body: Option<String>,
        /// Underlying HTTP client error when the request failed before a response.
        #[source]
        source: Option<SourceError>,
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
        id: i64,
    },

    /// The dataset has no original-file backup available for download.
    #[error("dataset {id} has no backup available")]
    NoBackup {
        /// Dataset id.
        id: i64,
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
        id: i64,
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
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Integer conversion failed.
    #[error("integer conversion failed")]
    IntegerConversion(#[from] TryFromIntError),
}

impl Error {
    /// Returns the HTTP status for API or storage responses that provided one.
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Api { status, .. } => Some(*status),
            Self::Storage { status, .. } => *status,
            _ => None,
        }
    }

    pub(crate) fn transport(
        method: &reqwest::Method,
        endpoint: impl Into<String>,
        source: reqwest::Error,
    ) -> Self {
        Self::Transport {
            method: method.as_str().to_string(),
            endpoint: endpoint.into(),
            source: SourceError::new(source),
        }
    }

    pub(crate) fn api(
        method: reqwest::Method,
        endpoint: impl Into<String>,
        status: reqwest::StatusCode,
        body: String,
    ) -> Self {
        Self::Api {
            method: method.as_str().to_string(),
            endpoint: endpoint.into(),
            status: status.as_u16(),
            body,
        }
    }

    pub(crate) fn storage(
        context: impl Into<String>,
        status: Option<reqwest::StatusCode>,
        body: Option<String>,
        source: Option<reqwest::Error>,
    ) -> Self {
        Self::Storage {
            context: context.into(),
            status: status.map(|status| status.as_u16()),
            body,
            source: source.map(SourceError::new),
        }
    }
}

/// Result type returned by the MarpleDB SDK.
pub type Result<T> = std::result::Result<T, Error>;
