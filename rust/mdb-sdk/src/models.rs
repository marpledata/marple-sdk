use crate::{NoopProgress, ProgressReporter};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt;
use std::sync::Arc;

/// JSON object used for user-defined stream or dataset metadata.
///
/// This is an insertion-preserving `serde_json::Map<String, Value>`.
pub type Metadata = Map<String, Value>;

/// Health response returned by the MarpleDB API.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Service health status.
    pub status: String,
}

/// MarpleDB stream metadata.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stream {
    /// Stream id.
    pub id: i32,
    /// Stream name.
    pub name: String,
    /// Stream type.
    #[serde(rename = "type")]
    pub stream_type: StreamType,
    /// Owning datapool.
    pub datapool: String,
    /// Stream description.
    #[serde(default, deserialize_with = "deserialize_default_string")]
    pub description: String,
    /// Number of datasets, if known.
    #[serde(default)]
    pub n_datasets: Option<u64>,
    /// Number of datapoints, if known.
    #[serde(default)]
    pub n_datapoints: Option<u64>,
    /// Cold-storage byte size, if known.
    #[serde(default)]
    pub cold_bytes: Option<u64>,
    /// Hot-storage byte size, if known.
    #[serde(default)]
    pub hot_bytes: Option<u64>,
    /// Import plugin name for file streams.
    #[serde(default)]
    pub plugin: Option<String>,
    /// Import plugin arguments for file streams.
    #[serde(default)]
    pub plugin_args: Option<String>,
    /// Additional stream fields returned by the API.
    #[serde(flatten)]
    pub extra: Value,
}

/// MarpleDB stream type.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamType {
    Files,
    Realtime,
}

fn deserialize_default_string<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

/// Dataset import lifecycle status.
///
/// Serialized values match the MarpleDB API and Python SDK enum names.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ImportStatus {
    /// File upload is still in progress.
    Uploading,
    /// Dataset is waiting to be imported.
    Waiting,
    /// Dataset import is running.
    Importing,
    /// Dataset post-processing is running.
    Postprocessing,
    /// Dataset post-processing failed.
    PostprocessingFailed,
    /// Dataset import finished successfully.
    Finished,
    /// Dataset is a live dataset.
    Live,
    /// Dataset import failed.
    Failed,
}

/// Dataset metadata returned by the MarpleDB API.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Dataset {
    /// Dataset id.
    pub id: i32,
    /// Owning stream id.
    pub datastream_id: i32,
    /// Owning stream version.
    pub datastream_version: i32,
    /// Creation timestamp as epoch seconds.
    pub created_at: f64,
    /// User that created the dataset, if available.
    pub created_by: Option<String>,
    /// Current import status.
    pub import_status: ImportStatus,
    /// Current import progress, if available.
    pub import_progress: Option<f64>,
    /// Import status message, if available.
    pub import_message: Option<String>,
    /// Import duration, if available.
    pub import_time: Option<f64>,
    /// Original dataset path or filename.
    pub path: String,
    /// User-defined dataset metadata.
    pub metadata: Metadata,
    /// Cold-storage path.
    pub cold_path: String,
    /// Cold-storage byte size.
    pub cold_bytes: Option<u64>,
    /// Hot-storage byte size.
    pub hot_bytes: Option<u64>,
    /// Backup path, if available.
    pub backup_path: Option<String>,
    /// Backup byte size, if available.
    pub backup_size: Option<u64>,
    /// Import plugin name.
    pub plugin: String,
    /// Import plugin arguments.
    pub plugin_args: String,
    /// Number of datapoints, if known.
    pub n_datapoints: Option<u64>,
    /// Number of signals, if known.
    pub n_signals: Option<u64>,
    /// Dataset start timestamp, if known.
    pub timestamp_start: Option<f64>,
    /// Dataset stop timestamp, if known.
    pub timestamp_stop: Option<f64>,
    /// Import speed, if known.
    pub import_speed: Option<f64>,
}

/// Upload mode preference for `MarpleDB::push_file`.
#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
pub enum UploadModeOverride {
    /// Let the server choose the upload mode.
    Auto,
    /// Force upload through the API server.
    Server,
}

/// Options for uploading a file.
#[non_exhaustive]
pub struct PushFileOptions {
    pub(crate) metadata: Metadata,
    pub(crate) concurrency: usize,
    pub(crate) upload_mode: UploadModeOverride,
    pub(crate) progress: Arc<dyn ProgressReporter>,
}

impl fmt::Debug for PushFileOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushFileOptions")
            .field("metadata", &self.metadata)
            .field("concurrency", &self.concurrency)
            .field("upload_mode", &self.upload_mode)
            .finish_non_exhaustive()
    }
}

impl PushFileOptions {
    /// Creates a builder for upload options.
    pub fn builder() -> PushFileOptionsBuilder {
        PushFileOptionsBuilder::default()
    }
}

impl Default for PushFileOptions {
    fn default() -> Self {
        Self {
            metadata: Default::default(),
            concurrency: 4,
            upload_mode: UploadModeOverride::Auto,
            progress: Arc::new(NoopProgress),
        }
    }
}

/// Builder for `PushFileOptions`.
#[non_exhaustive]
#[derive(Clone)]
pub struct PushFileOptionsBuilder {
    metadata: Metadata,
    concurrency: usize,
    upload_mode: UploadModeOverride,
    progress: Arc<dyn ProgressReporter>,
}

impl fmt::Debug for PushFileOptionsBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushFileOptionsBuilder")
            .field("metadata", &self.metadata)
            .field("concurrency", &self.concurrency)
            .field("upload_mode", &self.upload_mode)
            .finish_non_exhaustive()
    }
}

impl Default for PushFileOptionsBuilder {
    fn default() -> Self {
        let options = PushFileOptions::default();
        Self {
            metadata: options.metadata,
            concurrency: options.concurrency,
            upload_mode: options.upload_mode,
            progress: options.progress,
        }
    }
}

impl PushFileOptionsBuilder {
    /// Sets dataset metadata for the upload.
    ///
    /// ```
    /// use marple_db::PushFileOptions;
    /// use serde_json::json;
    ///
    /// let options = PushFileOptions::builder()
    ///     .metadata([
    ///         ("driver", json!("Mbaerto")),
    ///         ("run", json!(42)),
    ///     ])
    ///     .build();
    /// ```
    pub fn metadata<I, K, V>(mut self, entries: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Value>,
    {
        self.metadata = entries
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }

    /// Sets max concurrent part uploads for multipart modes.
    ///
    /// Higher values can improve throughput for large direct-storage uploads,
    /// but also increase memory use and the number of active storage requests.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Sets the upload mode preference.
    pub fn upload_mode(mut self, upload_mode: UploadModeOverride) -> Self {
        self.upload_mode = upload_mode;
        self
    }

    /// Sets the progress reporter.
    pub fn progress(mut self, progress: Arc<dyn ProgressReporter>) -> Self {
        self.progress = progress;
        self
    }

    /// Builds upload options.
    pub fn build(self) -> PushFileOptions {
        PushFileOptions {
            metadata: self.metadata,
            concurrency: self.concurrency,
            upload_mode: self.upload_mode,
            progress: self.progress,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct StreamsResponse {
    pub(crate) streams: Vec<Stream>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum UploadMode {
    Server,
    Azure,
    Single,
    Multipart,
}

#[derive(Debug, Deserialize)]
pub(crate) struct IngestionInit {
    pub(crate) dataset_id: i32,
    pub(crate) ingestion_id: i32,
    pub(crate) mode: UploadMode,
    pub(crate) presigned_url: Option<String>,
    pub(crate) part_size: Option<u64>,
    #[serde(rename = "expires_in")]
    pub(crate) _expires_in: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PartUrl {
    pub(crate) part_number: u32,
    pub(crate) url: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PartUrlsResponse {
    pub(crate) parts: Vec<PartUrl>,
    #[serde(rename = "expires_in")]
    pub(crate) _expires_in: u64,
    pub(crate) next_part: Option<u32>,
}
