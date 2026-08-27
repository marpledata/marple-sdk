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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Service health status.
    #[serde(default)]
    pub status: String,
}

/// Workspace settings returned by `/settings`.
///
/// Field names match the API's `SCREAMING_SNAKE_CASE` keys. Unknown keys are
/// preserved in [`Settings::extra`] so a server-side addition does not fail
/// deserialization. Known fields that the API omits or types unexpectedly
/// become `None`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Settings {
    #[serde(
        rename = "INSIGHT_URL",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub insight_url: Option<String>,
    #[serde(
        rename = "INSIGHT_DISTANCE_MODE_ENABLED",
        default,
        deserialize_with = "deserialize_opt_bool"
    )]
    pub insight_distance_mode_enabled: Option<bool>,
    #[serde(
        rename = "INSIGHT_DEFAULT_DISTANCE_SIGNAL",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub insight_default_distance_signal: Option<String>,
    #[serde(
        rename = "INSIGHT_SEGMENTS_ENABLED",
        default,
        deserialize_with = "deserialize_opt_bool"
    )]
    pub insight_segments_enabled: Option<bool>,
    #[serde(
        rename = "INSIGHT_SEGMENTS_SIGNAL",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub insight_segments_signal: Option<String>,
    #[serde(
        rename = "PATH_COLD",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub path_cold: Option<String>,
    #[serde(
        rename = "PATH_BACKUP",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub path_backup: Option<String>,
    #[serde(
        rename = "PATH_SWAP",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub path_swap: Option<String>,
    #[serde(
        rename = "PATH_LOGS",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub path_logs: Option<String>,
    #[serde(
        rename = "SUPERUSER_MANAGES_USERS",
        default,
        deserialize_with = "deserialize_opt_bool"
    )]
    pub superuser_manages_users: Option<bool>,
    #[serde(
        rename = "SANDBOX_JOBS_ENABLED",
        default,
        deserialize_with = "deserialize_opt_bool"
    )]
    pub sandbox_jobs_enabled: Option<bool>,
    #[serde(rename = "INGEST_PLUGINS_ADDITIONAL", default)]
    pub ingest_plugins_additional: Option<Value>,
    #[serde(
        rename = "DB_HOST",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub db_host: Option<String>,
    #[serde(rename = "DB_PORT", default, deserialize_with = "deserialize_opt_i64")]
    pub db_port: Option<i64>,
    #[serde(
        rename = "DB_NAME",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub db_name: Option<String>,
    #[serde(
        rename = "DB_USER",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub db_user: Option<String>,
    #[serde(
        rename = "INSIGHT_WEBHOOK_CONFIGURED",
        default,
        deserialize_with = "deserialize_opt_bool"
    )]
    pub insight_webhook_configured: Option<bool>,
    #[serde(
        rename = "VERSION",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub version: Option<String>,
    #[serde(
        rename = "GIT_HASH",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub git_hash: Option<String>,
    #[serde(
        rename = "BUILD_TIMESTAMP",
        default,
        deserialize_with = "deserialize_opt_string"
    )]
    pub build_timestamp: Option<String>,
    /// Additional settings keys returned by the API.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Workspace storage usage category used by `/usage/series/{usage_type}`.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageType {
    ColdStorage,
    HotStorage,
    ArchiveStorage,
    Import,
    ImportLive,
}

impl UsageType {
    /// Returns the API path segment for this usage type.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ColdStorage => "cold_storage",
            Self::HotStorage => "hot_storage",
            Self::ArchiveStorage => "archive_storage",
            Self::Import => "import",
            Self::ImportLive => "import_live",
        }
    }
}

impl fmt::Display for UsageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Time series returned by `/usage/series/{usage_type}`.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UsageSeries {
    /// Sample timestamps as epoch seconds.
    #[serde(default)]
    pub timestamps: Vec<f64>,
    /// Sample values. Storage series are in bytes.
    #[serde(default)]
    pub values: Vec<f64>,
    /// True when the series is a running integral (storage).
    #[serde(default)]
    pub integrated: bool,
    /// Unit advertised by the API, usually `bytes`.
    #[serde(default)]
    pub unit: String,
}

impl UsageSeries {
    /// Returns the latest sample as a non-negative integer, if present.
    pub fn latest(&self) -> Option<u64> {
        let value = self.values.last().copied()?;
        if !value.is_finite() {
            return None;
        }
        Some(value.round().clamp(0.0, u64::MAX as f64) as u64)
    }
}

/// Byte quota from a workspace license.
///
/// The API encodes unlimited as a negative limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "i64", into = "i64")]
pub enum StorageQuota {
    /// No byte cap (`limit < 0`).
    Unlimited,
    /// Finite cap in bytes (`limit >= 0`).
    Bytes(u64),
}

impl From<i64> for StorageQuota {
    fn from(limit: i64) -> Self {
        if limit < 0 {
            Self::Unlimited
        } else {
            Self::Bytes(limit as u64)
        }
    }
}

impl From<StorageQuota> for i64 {
    fn from(quota: StorageQuota) -> Self {
        match quota {
            StorageQuota::Unlimited => -1,
            StorageQuota::Bytes(bytes) => i64::try_from(bytes).unwrap_or(i64::MAX),
        }
    }
}

/// License type issued for a MarpleDB workspace.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LicenseType {
    Dev,
    Free,
    Trial,
    Paid,
    Poc,
    Sponsorship,
    #[default]
    #[serde(other)]
    Unknown,
}

/// Realtime ingest tier from the workspace license.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RealtimeTier {
    Disabled,
    Slow,
    Fast,
    Unlimited,
    #[default]
    #[serde(other)]
    Unknown,
}

/// Storage and ingest limits from a workspace license.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LicenseLimits {
    #[serde(default)]
    pub hot_bytes: Option<StorageQuota>,
    #[serde(default)]
    pub cold_bytes: Option<StorageQuota>,
    #[serde(default)]
    pub archive_bytes: Option<StorageQuota>,
    #[serde(default, deserialize_with = "deserialize_opt_i64")]
    pub ingestion_workers: Option<i64>,
    #[serde(default)]
    pub realtime: Option<RealtimeTier>,
}

/// Signed license payload returned by `/workspace/license`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LicensePayload {
    #[serde(rename = "type", default)]
    pub license_type: LicenseType,
    #[serde(default)]
    pub product: String,
    #[serde(default)]
    pub deployment: String,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub expiry_date: Option<i64>,
    #[serde(default)]
    pub features: LicenseLimits,
}

/// Workspace license returned by `/workspace/license`.
///
/// `id` is the license row id. `workspace` is the workspace slug.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceLicense {
    #[serde(default, deserialize_with = "deserialize_opt_i64")]
    pub id: Option<i64>,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub issued_at: Option<i64>,
    #[serde(default)]
    pub cached_at: Option<i64>,
    #[serde(default)]
    pub workspace: String,
    #[serde(default)]
    pub payload: LicensePayload,
}

impl WorkspaceLicense {
    /// Workspace slug from the license row, or the payload copy when the row is empty.
    pub fn workspace_id(&self) -> Option<&str> {
        let slug = self.workspace.as_str();
        if !slug.is_empty() {
            return Some(slug);
        }
        self.payload
            .workspace
            .as_deref()
            .filter(|id| !id.is_empty())
    }
}

/// Workspace membership returned by `/user/info`.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkspaceMembership {
    #[serde(default)]
    pub workspace_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub role: String,
    #[serde(default, deserialize_with = "deserialize_optional_epoch")]
    pub last_active: Option<i64>,
}

/// Current user profile returned by `/user/info`.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserInfo {
    #[serde(default, deserialize_with = "deserialize_i64")]
    pub id: i64,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub workspaces: Vec<WorkspaceMembership>,
    #[serde(default)]
    pub license: Option<WorkspaceLicense>,
    #[serde(flatten)]
    pub extra: Value,
}

impl UserInfo {
    /// Slug of the workspace bound to this token.
    ///
    /// Prefers `license.workspace`, then `license.payload.workspace`, then the
    /// sole membership when the user belongs to exactly one workspace.
    pub fn current_workspace_id(&self) -> Option<&str> {
        self.license
            .as_ref()
            .and_then(WorkspaceLicense::workspace_id)
            .or_else(|| match self.workspaces.as_slice() {
                [membership] if !membership.workspace_id.is_empty() => {
                    Some(membership.workspace_id.as_str())
                }
                _ => None,
            })
    }

    /// Display name for `workspace_id`, or the slug when no membership matches.
    pub fn workspace_name<'a>(&'a self, workspace_id: &'a str) -> &'a str {
        self.workspaces
            .iter()
            .find(|membership| membership.workspace_id == workspace_id)
            .map(|membership| membership.name.as_str())
            .filter(|name| !name.is_empty())
            .unwrap_or(workspace_id)
    }
}

/// Resolved current workspace for the connected token.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentWorkspace {
    /// Workspace slug (`AuthWorkspace.id`).
    pub id: String,
    /// Display name, or the slug when `/user/info` has no match.
    pub name: String,
    /// License from `/user/info`, including storage quotas.
    pub license: Option<WorkspaceLicense>,
    pub cold_bytes: Option<u64>,
    pub hot_bytes: Option<u64>,
    pub archive_bytes: Option<u64>,
}

/// MarpleDB stream metadata.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stream {
    /// Stream id.
    #[serde(default, deserialize_with = "deserialize_i64")]
    pub id: i64,
    /// Stream name.
    #[serde(default)]
    pub name: String,
    /// Stream type. Unknown API values become [`StreamType::Unknown`].
    #[serde(rename = "type", default)]
    pub stream_type: StreamType,
    /// Owning datapool.
    #[serde(default)]
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamType {
    Files,
    Realtime,
    #[default]
    #[serde(other)]
    Unknown,
}

/// Dataset import lifecycle status.
///
/// Serialized values match the MarpleDB API and Python SDK enum names.
/// Unknown API values become [`ImportStatus::Unknown`] instead of failing.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Realtime dataset cooling is in progress.
    Cooling,
    /// Realtime dataset cooling failed.
    CoolingFailed,
    /// Status the SDK does not know about yet.
    #[default]
    #[serde(other)]
    Unknown,
}

impl ImportStatus {
    /// API name for this status, such as `FINISHED` or `COOLING_FAILED`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Uploading => "UPLOADING",
            Self::Waiting => "WAITING",
            Self::Importing => "IMPORTING",
            Self::Postprocessing => "POSTPROCESSING",
            Self::PostprocessingFailed => "POSTPROCESSING_FAILED",
            Self::Finished => "FINISHED",
            Self::Live => "LIVE",
            Self::Failed => "FAILED",
            Self::Cooling => "COOLING",
            Self::CoolingFailed => "COOLING_FAILED",
            Self::Unknown => "UNKNOWN",
        }
    }

    /// `true` when import completed successfully (`Finished` or `Live`).
    pub fn is_success(self) -> bool {
        matches!(self, Self::Finished | Self::Live)
    }

    /// `true` when import reached a failed terminal status.
    pub fn is_failure(self) -> bool {
        matches!(
            self,
            Self::Failed | Self::PostprocessingFailed | Self::CoolingFailed
        )
    }
}

impl fmt::Display for ImportStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Dataset metadata returned by the MarpleDB API.
///
/// Matches the backend `File` row returned by dataset list/get endpoints.
/// Optional fields default when omitted; unknown keys land in [`Dataset::extra`].
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Dataset {
    /// Dataset id.
    #[serde(default, deserialize_with = "deserialize_i64")]
    pub id: i64,
    /// Owning stream id. Accepts `stream_id` as an alias.
    #[serde(default, alias = "stream_id", deserialize_with = "deserialize_i64")]
    pub datastream_id: i64,
    /// Owning stream version.
    #[serde(default, deserialize_with = "deserialize_opt_i64")]
    pub datastream_version: Option<i64>,
    /// Ingestion record id, if the API returned one.
    #[serde(default, deserialize_with = "deserialize_opt_i64")]
    pub import_id: Option<i64>,
    /// Creation timestamp as epoch seconds.
    #[serde(default)]
    pub created_at: f64,
    /// User that created the dataset, if available.
    #[serde(default)]
    pub created_by: Option<String>,
    /// Current import status.
    #[serde(default)]
    pub import_status: ImportStatus,
    /// Current import progress, if available.
    #[serde(default)]
    pub import_progress: Option<f64>,
    /// Import status message, if available.
    #[serde(default)]
    pub import_message: Option<String>,
    /// Import duration, if available.
    #[serde(default)]
    pub import_time: Option<f64>,
    /// Original dataset path or filename.
    #[serde(default)]
    pub path: String,
    /// User-defined dataset metadata.
    #[serde(default)]
    pub metadata: Metadata,
    /// Cold-storage path.
    #[serde(default)]
    pub cold_path: Option<String>,
    /// Cold-storage byte size.
    #[serde(default)]
    pub cold_bytes: Option<u64>,
    /// Hot-storage byte size.
    #[serde(default)]
    pub hot_bytes: Option<u64>,
    /// Backup path, if available.
    #[serde(default)]
    pub backup_path: Option<String>,
    /// Backup byte size, if available.
    #[serde(default)]
    pub backup_size: Option<u64>,
    /// Import plugin name.
    #[serde(default)]
    pub plugin: Option<String>,
    /// Import plugin arguments.
    #[serde(default)]
    pub plugin_args: Option<String>,
    /// Number of datapoints, if known.
    #[serde(default)]
    pub n_datapoints: Option<u64>,
    /// Number of signals, if known.
    #[serde(default)]
    pub n_signals: Option<u64>,
    /// Dataset start timestamp, if known.
    #[serde(default)]
    pub timestamp_start: Option<f64>,
    /// Dataset stop timestamp, if known.
    #[serde(default)]
    pub timestamp_stop: Option<f64>,
    /// Import speed, if known.
    #[serde(default)]
    pub import_speed: Option<f64>,
    /// Parquet format version, if known.
    #[serde(default, deserialize_with = "deserialize_opt_i64")]
    pub parquet_version: Option<i64>,
    /// Additional dataset fields returned by the API.
    #[serde(flatten)]
    pub extra: Value,
}

/// Storage lifecycle of a signal.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StorageStatus {
    FrozenToCold,
    Cold,
    ColdToHot,
    Hot,
    #[default]
    #[serde(other)]
    Unknown,
}

/// Signal metadata returned by the MarpleDB API.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Signal {
    /// Signal id.
    #[serde(default, deserialize_with = "deserialize_i64")]
    pub id: i64,
    /// Signal name.
    #[serde(default)]
    pub name: String,
    /// Engineering unit, if set.
    #[serde(default)]
    pub unit: Option<String>,
    /// Description, if set.
    #[serde(default)]
    pub description: Option<String>,
    /// User-defined signal metadata.
    #[serde(default)]
    pub metadata: Metadata,
    /// Current storage status.
    #[serde(default)]
    pub storage_status: StorageStatus,
    /// Cold-storage byte size, if known.
    #[serde(default)]
    pub cold_bytes: Option<u64>,
    /// Hot-storage byte size, if known.
    #[serde(default)]
    pub hot_bytes: Option<u64>,
    /// Number of samples, if known.
    #[serde(default)]
    pub count: Option<u64>,
    /// Aggregate stats, if the API returned them.
    #[serde(default)]
    pub stats: Option<Value>,
    /// Numeric sample count, if known.
    #[serde(default)]
    pub count_value: Option<u64>,
    /// Text sample count, if known.
    #[serde(default)]
    pub count_text: Option<u64>,
    /// First timestamp (nanoseconds), if known.
    #[serde(default)]
    pub time_min: Option<i64>,
    /// Last timestamp (nanoseconds), if known.
    #[serde(default)]
    pub time_max: Option<i64>,
    /// Parquet format version, if known.
    #[serde(default, deserialize_with = "deserialize_opt_i64")]
    pub parquet_version: Option<i64>,
    /// Owning stream id.
    #[serde(default, deserialize_with = "deserialize_opt_i64")]
    pub datastream_id: Option<i64>,
    /// Owning dataset id.
    #[serde(default, deserialize_with = "deserialize_opt_i64")]
    pub dataset_id: Option<i64>,
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
    pub(crate) dataset_name: Option<String>,
    pub(crate) concurrency: usize,
    pub(crate) upload_mode: UploadModeOverride,
    pub(crate) progress: Arc<dyn ProgressReporter>,
    pub(crate) overwrite: bool,
}

impl fmt::Debug for PushFileOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushFileOptions")
            .field("metadata", &self.metadata)
            .field("dataset_name", &self.dataset_name)
            .field("concurrency", &self.concurrency)
            .field("upload_mode", &self.upload_mode)
            .field("overwrite", &self.overwrite)
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
            dataset_name: None,
            concurrency: 4,
            upload_mode: UploadModeOverride::Auto,
            progress: Arc::new(NoopProgress),
            overwrite: false,
        }
    }
}

/// Builder for `PushFileOptions`.
#[non_exhaustive]
#[derive(Clone)]
pub struct PushFileOptionsBuilder {
    metadata: Metadata,
    dataset_name: Option<String>,
    concurrency: usize,
    upload_mode: UploadModeOverride,
    progress: Arc<dyn ProgressReporter>,
    overwrite: bool,
}

impl fmt::Debug for PushFileOptionsBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushFileOptionsBuilder")
            .field("metadata", &self.metadata)
            .field("dataset_name", &self.dataset_name)
            .field("concurrency", &self.concurrency)
            .field("upload_mode", &self.upload_mode)
            .field("overwrite", &self.overwrite)
            .finish_non_exhaustive()
    }
}

impl Default for PushFileOptionsBuilder {
    fn default() -> Self {
        let options = PushFileOptions::default();
        Self {
            metadata: options.metadata,
            dataset_name: options.dataset_name,
            concurrency: options.concurrency,
            upload_mode: options.upload_mode,
            progress: options.progress,
            overwrite: options.overwrite,
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

    /// Sets the dataset name for the upload.
    ///
    /// Must be unique per datapool, or set `overwrite` to true.
    pub fn dataset_name(mut self, dataset_name: impl Into<String>) -> Self {
        self.dataset_name = Some(dataset_name.into());
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

    /// Set true to overwrite an existing dataset with the same name.
    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Builds upload options.
    pub fn build(self) -> PushFileOptions {
        PushFileOptions {
            metadata: self.metadata,
            dataset_name: self.dataset_name,
            concurrency: self.concurrency,
            upload_mode: self.upload_mode,
            progress: self.progress,
            overwrite: self.overwrite,
        }
    }
}

fn deserialize_default_string<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

/// `/user/info` sends `EXTRACT(EPOCH FROM last_active)`, a Postgres float or JSON null.
fn deserialize_optional_epoch<'de, D>(deserializer: D) -> std::result::Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_opt_i64(deserializer)
}

pub(crate) fn deserialize_i64<'de, D>(deserializer: D) -> std::result::Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    i64_from_value(Value::deserialize(deserializer)?)
        .ok_or_else(|| serde::de::Error::custom("expected a number"))
}

fn deserialize_opt_i64<'de, D>(deserializer: D) -> std::result::Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<Value>::deserialize(deserializer)? {
        None | Some(Value::Null) => None,
        Some(value) => i64_from_value(value),
    })
}

fn deserialize_opt_string<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<Value>::deserialize(deserializer)? {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text).filter(|text| !text.is_empty()),
        Some(Value::Number(number)) => Some(number.to_string()),
        Some(Value::Bool(value)) => Some(value.to_string()),
        Some(_) => None,
    })
}

fn deserialize_opt_bool<'de, D>(deserializer: D) -> std::result::Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<Value>::deserialize(deserializer)? {
        None | Some(Value::Null) => None,
        Some(Value::Bool(value)) => Some(value),
        Some(Value::Number(number)) => number.as_i64().map(|value| value != 0).or_else(|| {
            number
                .as_f64()
                .filter(|value| value.is_finite())
                .map(|value| value != 0.0)
        }),
        Some(Value::String(text)) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        Some(_) => None,
    })
}

fn i64_from_value(value: Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok()))
            .or_else(|| number.as_f64().and_then(epoch_from_f64)),
        Value::String(text) => text
            .parse::<i64>()
            .ok()
            .or_else(|| text.parse::<f64>().ok().and_then(epoch_from_f64)),
        _ => None,
    }
}

fn epoch_from_f64(value: f64) -> Option<i64> {
    value.is_finite().then(|| value.round() as i64)
}

#[cfg(test)]
mod import_status_tests {
    use super::ImportStatus;

    #[test]
    fn every_variant_has_a_name_and_one_classification() {
        let statuses = [
            ImportStatus::Uploading,
            ImportStatus::Waiting,
            ImportStatus::Importing,
            ImportStatus::Postprocessing,
            ImportStatus::PostprocessingFailed,
            ImportStatus::Finished,
            ImportStatus::Live,
            ImportStatus::Failed,
            ImportStatus::Cooling,
            ImportStatus::CoolingFailed,
            ImportStatus::Unknown,
        ];

        for status in statuses {
            let (name, success, failure) = match status {
                ImportStatus::Uploading => ("UPLOADING", false, false),
                ImportStatus::Waiting => ("WAITING", false, false),
                ImportStatus::Importing => ("IMPORTING", false, false),
                ImportStatus::Postprocessing => ("POSTPROCESSING", false, false),
                ImportStatus::PostprocessingFailed => ("POSTPROCESSING_FAILED", false, true),
                ImportStatus::Finished => ("FINISHED", true, false),
                ImportStatus::Live => ("LIVE", true, false),
                ImportStatus::Failed => ("FAILED", false, true),
                ImportStatus::Cooling => ("COOLING", false, false),
                ImportStatus::CoolingFailed => ("COOLING_FAILED", false, true),
                ImportStatus::Unknown => ("UNKNOWN", false, false),
            };

            assert_eq!(status.as_str(), name);
            assert_eq!(status.to_string(), name);
            assert_eq!(status.is_success(), success);
            assert_eq!(status.is_failure(), failure);
            assert!(!(success && failure));
        }
    }
}
