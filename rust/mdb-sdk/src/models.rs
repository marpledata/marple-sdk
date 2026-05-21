use crate::ProgressReporter;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub type Metadata = HashMap<String, Value>;

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Stream {
    pub id: i32,
    pub name: String,
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Dataset {
    pub id: i32,
    pub datastream_id: i32,
    pub datastream_version: i32,
    pub created_at: f64,
    pub created_by: Option<String>,
    pub import_status: String,
    pub import_progress: Option<f64>,
    pub import_message: Option<String>,
    pub import_time: Option<f64>,
    pub path: String,
    pub metadata: Metadata,
    pub cold_path: String,
    pub cold_bytes: Option<u64>,
    pub hot_bytes: Option<u64>,
    pub backup_path: Option<String>,
    pub backup_size: Option<u64>,
    pub plugin: String,
    pub plugin_args: String,
    pub n_datapoints: Option<u64>,
    pub n_signals: Option<u64>,
    pub timestamp_start: Option<f64>,
    pub timestamp_stop: Option<f64>,
    pub import_speed: Option<f64>,
}

#[derive(Clone, Copy, Debug)]
pub enum UploadModeOverride {
    Auto,
    Server,
}

pub struct PushFileOptions {
    pub metadata: Metadata,
    pub concurrency: usize,
    pub upload_mode: UploadModeOverride,
    pub progress: Arc<dyn ProgressReporter>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BackupResponse {
    pub(crate) path: String,
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
