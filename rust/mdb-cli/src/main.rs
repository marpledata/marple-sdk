use anyhow::{Result, anyhow};
use base64::Engine;
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use colored::*;
use futures_util::{StreamExt, lock::Mutex};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{
    Body, Client, Response,
    header::{AUTHORIZATION, CONTENT_LENGTH, HeaderMap, HeaderValue},
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;
use walkdir::WalkDir;

const PROGRESS_STREAM_CHUNK: usize = 256 * 1024;

fn progress_bar(message: &str, total_size: u64) -> Result<ProgressBar> {
    let bar = ProgressBar::new(total_size);
    bar.set_style(ProgressStyle::default_bar().template(
        "- {msg} [{wide_bar}] ({binary_bytes_per_sec}, eta {eta}) {binary_bytes}/{binary_total_bytes}",
    )?.progress_chars("=> "));
    bar.set_message(message.to_string());
    Ok(bar)
}

fn progress_reporting_stream(
    data: Vec<u8>,
    uploaded: Arc<AtomicU64>,
    bar: ProgressBar,
) -> impl futures_util::Stream<Item = std::io::Result<Vec<u8>>> + 'static {
    async_stream::stream! {
        let mut pos = 0;
        while pos < data.len() {
            let end = (pos + PROGRESS_STREAM_CHUNK).min(data.len());
            let chunk = data[pos..end].to_vec();
            let chunk_len = chunk.len() as u64;
            let new_uploaded = uploaded.fetch_add(chunk_len, Ordering::Relaxed) + chunk_len;
            bar.set_position(new_uploaded);
            yield Ok(chunk);
            pos = end;
        }
    }
}

async fn ensure_success(response: Response, failure_message: impl Into<String>) -> Result<()> {
    if response.status().is_success() {
        Ok(())
    } else {
        Err(anyhow!(
            "{} with status {}: {}",
            failure_message.into(),
            response.status(),
            response.text().await?
        ))
    }
}

#[derive(Parser)]
#[command(name = "mdb")]
#[command(about = "MarpleDB CLI - Interact with MarpleDB API")]
#[command()]
struct Cli {
    #[arg(
        long,
        default_value = "https://db.marpledata.com/api/v1",
        env = "MDB_URL"
    )]
    mdb_url: String,

    #[arg(long, default_value = "", env = "MDB_TOKEN")]
    mdb_token: String,

    #[arg(long)]
    version: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Ping the MarpleDB API
    Ping,

    /// Stream commands
    Stream {
        #[command(subcommand)]
        command: StreamCommands,
    },

    /// Ingest files into a stream
    Ingest {
        /// Stream name
        stream_name: String,

        /// Metadata key=value pairs
        #[arg(short, long, value_parser = parse_key_val)]
        metadata: Vec<(String, Value)>,

        /// Files or directories to ingest
        files: Vec<PathBuf>,

        /// Recursively process directories
        #[arg(short, long)]
        recursive: bool,

        /// Only ingest files with this extension
        #[arg(short, long)]
        extension: Option<String>,

        /// Skip existing datasets
        #[arg(short, long)]
        skip_existing: bool,

        /// Max concurrent multipart part uploads
        #[arg(long, default_value_t = 4)]
        concurrency: usize,

        /// Upload mode override
        #[arg(long, value_enum, default_value_t = UploadModeOverride::Auto)]
        upload_mode: UploadModeOverride,
    },

    /// Dataset commands
    Dataset {
        /// Stream name
        stream_name: String,

        #[command(subcommand)]
        command: DatasetCommands,
    },

    /// GET a MarpleDB API endpoint
    Get {
        /// API endpoint
        endpoint: String,

        /// Query parameters (key=value)
        #[arg(num_args = 0.., value_parser = parse_key_val)]
        params: Vec<(String, Value)>,
    },

    /// POST to a MarpleDB API endpoint
    Post {
        /// API endpoint
        endpoint: String,

        /// Data parameters (key=value)
        #[arg(num_args = 0.., value_parser = parse_key_val)]
        data: Vec<(String, Value)>,
    },

    /// DELETE a MarpleDB API endpoint
    Delete {
        /// API endpoint
        endpoint: String,

        /// Data parameters (key=value)
        #[arg(num_args = 0.., value_parser = parse_key_val)]
        data: Vec<(String, Value)>,
    },
}

#[derive(Subcommand)]
enum StreamCommands {
    /// List all streams
    List,

    /// Get a stream
    Get {
        /// Stream name
        stream_name: String,
    },

    /// Create a new stream
    New {
        /// Stream name
        stream_name: String,

        /// Stream properties (key=value)
        #[arg(num_args = 0.., value_parser = parse_key_val)]
        properties: Vec<(String, Value)>,
    },

    /// Update a stream
    Update {
        /// Stream name
        stream_name: String,

        /// Stream properties (key=value)
        #[arg(num_args = 0.., value_parser = parse_key_val)]
        properties: Vec<(String, Value)>,
    },
}

#[derive(Subcommand)]
enum DatasetCommands {
    /// List all datasets in a stream
    List,

    /// Get a dataset
    Get {
        /// Dataset ID
        dataset_id: i32,
    },

    /// Download a dataset
    Download {
        /// Output directory
        #[arg(short, long)]
        output_dir: Option<String>,

        /// Dataset ID
        dataset_id: Option<i32>,
    },
}

fn parse_key_val(s: &str) -> Result<(String, Value)> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() == 2 {
        let value: Value =
            serde_json::from_str(parts[1]).unwrap_or(Value::String(parts[1].to_string()));
        Ok((parts[0].to_string(), value))
    } else {
        Err(anyhow!("invalid KEY=value: no `=` found in `{}`", s))
    }
}

fn to_record(pairs: Vec<(String, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().collect()
}

#[derive(Debug, Serialize, Deserialize)]
struct HealthResponse {
    status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Stream {
    id: i32,
    name: String,
    #[serde(flatten)]
    extra: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct Dataset {
    id: i32,
    datastream_id: i32,
    datastream_version: i32,
    created_at: f64,
    created_by: Option<String>,
    import_status: String,
    import_progress: Option<f64>,
    import_message: Option<String>,
    import_time: Option<f64>,
    path: String,
    metadata: HashMap<String, Value>,
    cold_path: String,
    cold_bytes: Option<u64>,
    hot_bytes: Option<u64>,
    backup_path: Option<String>,
    backup_size: Option<u64>,
    plugin: String,
    plugin_args: String,
    n_datapoints: Option<u64>,
    n_signals: Option<u64>,
    timestamp_start: Option<f64>,
    timestamp_stop: Option<f64>,
    import_speed: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BackupResponse {
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StreamsResponse {
    streams: Vec<Stream>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum UploadMode {
    Server,
    Azure,
    Single,
    Multipart,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[clap(rename_all = "lowercase")]
enum UploadModeOverride {
    Auto,
    Server,
}

#[derive(Debug, Deserialize)]
struct IngestionInit {
    dataset_id: i32,
    ingestion_id: i32,
    mode: UploadMode,
    presigned_url: Option<String>,
    part_size: Option<u64>,
    #[serde(rename = "expires_in")]
    _expires_in: u64,
}

#[derive(Debug, Deserialize)]
struct PartUrl {
    part_number: u32,
    url: String,
}

#[derive(Debug, Deserialize)]
struct PartUrlsResponse {
    parts: Vec<PartUrl>,
    #[serde(rename = "expires_in")]
    _expires_in: u64,
    next_part: Option<u32>,
}

#[derive(Clone)]
struct MultipartUploadContext {
    storage_client: Client,
    file_path: PathBuf,
    part_size: u64,
    total_size: u64,
    uploaded: Arc<AtomicU64>,
    bar: ProgressBar,
}

#[derive(Clone)]
struct BlockDescriptor {
    offset: u64,
    length: u64,
    block_id: String,
}

#[derive(Clone)]
struct AzureBlockUploadContext {
    storage_client: Client,
    sas_url: Arc<reqwest::Url>,
    file_path: PathBuf,
    uploaded: Arc<AtomicU64>,
    bar: ProgressBar,
}

fn azure_block_descriptors(total_size: u64, block_size: u64) -> Vec<BlockDescriptor> {
    if total_size == 0 {
        return Vec::new();
    }

    let n_blocks = total_size.div_ceil(block_size);
    (0..n_blocks as u32)
        .map(|block_number| {
            let offset = u64::from(block_number) * block_size;
            let length = block_size.min(total_size - offset);
            BlockDescriptor {
                offset,
                length,
                block_id: format!("{block_number:08}"),
            }
        })
        .collect()
}

struct MarpleDB {
    client: Client,
    storage_client: Client,
    base_url: String,
}

impl MarpleDB {
    fn new(url: &str, token: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        let mut bearer = HeaderValue::from_str(&format!("Bearer {}", token))?;
        bearer.set_sensitive(true);
        headers.insert(AUTHORIZATION, bearer);
        let client = Client::builder().default_headers(headers).build()?;
        // Direct storage URLs are pre-signed/SAS-authenticated, so do not send Marple DB headers.
        let storage_client = Client::new();

        Ok(Self {
            client,
            storage_client,
            base_url: url.trim_end_matches('/').to_string() + "/",
        })
    }

    async fn handle_response(
        &self,
        endpoint: &str,
        method: &str,
        response: Response,
    ) -> Result<Value> {
        if response.status().is_success() {
            let json = response.json().await?;
            Ok(json)
        } else {
            Err(anyhow!(
                "{} {} failed with status {}: {}",
                method,
                endpoint,
                response.status(),
                response.text().await?
            ))
        }
    }

    async fn get(&self, endpoint: &str, params: Option<Vec<(String, Value)>>) -> Result<Value> {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let mut request = self.client.get(&url);

        if let Some(params) = params {
            request = request.query(&to_record(params));
        }
        let response = request.send().await?;
        self.handle_response(endpoint, "GET", response).await
    }

    async fn post(&self, endpoint: &str, data: Option<Vec<(String, Value)>>) -> Result<Value> {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let mut request = self.client.post(&url);

        if let Some(data) = data {
            request = request.json(&to_record(data));
        }
        let response = request.send().await?;
        self.handle_response(endpoint, "POST", response).await
    }

    async fn delete(&self, endpoint: &str, json: Option<Vec<(String, Value)>>) -> Result<Value> {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let mut request = self.client.post(&url);
        if let Some(json) = json {
            request = request.json(&to_record(json));
        }

        let response = request.send().await?;
        self.handle_response(endpoint, "DELETE", response).await
    }

    async fn post_json<B, R>(&self, endpoint: &str, body: &B) -> Result<R>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let response = self.client.post(url).json(body).send().await?;
        let response = self.handle_response(endpoint, "POST", response).await?;
        Ok(serde_json::from_value(response)?)
    }

    async fn post_multipart(
        &self,
        endpoint: &str,
        form: reqwest::multipart::Form,
    ) -> Result<Value> {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let response = self.client.post(url).multipart(form).send().await?;
        self.handle_response(endpoint, "POST", response).await
    }

    async fn get_json<Q, R>(&self, endpoint: &str, query: &Q) -> Result<R>
    where
        Q: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let response = self.client.get(url).query(query).send().await?;
        let response = self.handle_response(endpoint, "GET", response).await?;
        Ok(serde_json::from_value(response)?)
    }

    async fn health(&self) -> Result<HealthResponse> {
        let response = self.get("health", None).await?;
        Ok(serde_json::from_value(response)?)
    }

    async fn get_streams(&self) -> Result<Vec<Stream>> {
        let response = self.get("streams", None).await?;
        let streams_response: StreamsResponse = serde_json::from_value(response)?;
        Ok(streams_response.streams)
    }

    async fn get_stream(&self, stream_name: &str) -> Result<Stream> {
        let streams = self.get_streams().await?;
        if let Some(stream) = streams.into_iter().find(|s| s.name == stream_name) {
            Ok(stream)
        } else {
            Err(anyhow!("stream {} not found", stream_name))
        }
    }

    async fn create_stream(&self, stream_name: &str, options: &[(String, Value)]) -> Result<Value> {
        let mut options = options.to_vec();
        options.push(("name".to_string(), Value::String(stream_name.to_string())));
        self.post("stream", Some(options)).await
    }

    async fn update_stream(&self, stream_id: i32, options: &[(String, Value)]) -> Result<Value> {
        let endpoint = format!("stream/update/{}", stream_id);
        self.post(&endpoint, Some(options.to_vec())).await
    }

    async fn get_datasets(&self, stream_id: i32) -> Result<Vec<Dataset>> {
        let response = self
            .get(&format!("stream/{}/datasets", stream_id), None)
            .await?;
        let datasets: Vec<Dataset> = response
            .as_array()
            .ok_or(anyhow!("Expected an array of datasets"))?
            .iter()
            .map(|d| serde_json::from_value(d.clone()))
            .collect::<std::result::Result<_, _>>()?;
        Ok(datasets)
    }

    async fn get_dataset(&self, stream_id: i32, dataset_id: i32) -> Result<Dataset> {
        let response = self
            .get(
                &format!("stream/{}/dataset/{}", stream_id, dataset_id),
                None,
            )
            .await?;
        let dataset: Dataset = serde_json::from_value(response)?;
        Ok(dataset)
    }

    async fn init_ingestion(
        &self,
        stream_id: i32,
        dataset_name: &str,
        file_size: u64,
        metadata: &HashMap<String, Value>,
    ) -> Result<IngestionInit> {
        let body = serde_json::json!({
            "stream_id": stream_id,
            "dataset_name": dataset_name,
            "file_size": file_size,
            "metadata": metadata,
        });
        self.post_json("ingestion", &body).await
    }

    async fn get_part_urls(
        &self,
        ingestion_id: i32,
        start_part: u32,
        count: usize,
    ) -> Result<PartUrlsResponse> {
        let endpoint = format!("ingestion/{}/upload/part-urls", ingestion_id);
        self.get_json(
            &endpoint,
            &[("start_part", start_part), ("count", count as u32)],
        )
        .await
    }

    async fn complete_upload(&self, ingestion_id: i32) -> Result<()> {
        let endpoint = format!("ingestion/{}/upload/complete", ingestion_id);
        self.post_json::<_, Value>(&endpoint, &serde_json::json!({}))
            .await?;
        Ok(())
    }

    async fn abort_upload(&self, ingestion_id: i32, reason: &str) -> Result<()> {
        let endpoint = format!("ingestion/{}/abort", ingestion_id);
        self.post_json::<_, Value>(&endpoint, &serde_json::json!({ "reason": reason }))
            .await?;
        Ok(())
    }

    fn upload_progress_bar(file_name: &str, total_size: u64) -> Result<ProgressBar> {
        progress_bar(file_name, total_size)
    }

    async fn upload_via_single(
        &self,
        init: &IngestionInit,
        file_path: &Path,
        file_name: &str,
        total_size: u64,
    ) -> Result<()> {
        let url = init
            .presigned_url
            .as_deref()
            .ok_or_else(|| anyhow!("single upload mode without presigned_url"))?;
        let file = tokio::fs::File::open(file_path).await?;
        let mut uploaded = 0;
        let bar = Self::upload_progress_bar(file_name, total_size)?;

        let mut reader = ReaderStream::new(file);
        let stream = async_stream::stream! {
            while let Some(chunk) = reader.next().await {
                if let Ok(chunk) = &chunk {
                    uploaded += chunk.len() as u64;
                    bar.set_position(uploaded);
                }
                yield chunk;
            }
            bar.finish_and_clear();
        };

        let response = self
            .storage_client
            .put(url)
            .header(CONTENT_LENGTH, total_size)
            .body(Body::wrap_stream(stream))
            .send()
            .await?;
        ensure_success(response, "storage PUT failed").await?;
        Ok(())
    }

    async fn upload_via_server(
        &self,
        init: &IngestionInit,
        file_path: &Path,
        file_name: &str,
        total_size: u64,
    ) -> Result<()> {
        let file = tokio::fs::File::open(file_path).await?;
        let mut uploaded = 0;
        let bar = Self::upload_progress_bar(file_name, total_size)?;

        let mut reader = ReaderStream::new(file);
        let stream = async_stream::stream! {
            while let Some(chunk) = reader.next().await {
                if let Ok(chunk) = &chunk {
                    uploaded += chunk.len() as u64;
                    bar.set_position(uploaded);
                }
                yield chunk;
            }
            bar.finish_and_clear();
        };

        let body = Body::wrap_stream(stream);
        let part = reqwest::multipart::Part::stream_with_length(body, total_size)
            .file_name(file_name.to_string())
            .mime_str("application/octet-stream")?;
        let form = reqwest::multipart::Form::new().part("file", part);
        let endpoint = format!("ingestion/{}/upload/server", init.ingestion_id);
        self.post_multipart(&endpoint, form).await?;
        Ok(())
    }

    async fn put_block(
        file: &mut tokio::fs::File,
        context: &AzureBlockUploadContext,
        descriptor: BlockDescriptor,
    ) -> Result<()> {
        file.seek(SeekFrom::Start(descriptor.offset)).await?;
        let mut data = vec![0; usize::try_from(descriptor.length)?];
        file.read_exact(&mut data).await?;

        let stream =
            progress_reporting_stream(data, Arc::clone(&context.uploaded), context.bar.clone());

        let mut block_url = (*context.sas_url).clone();
        block_url
            .query_pairs_mut()
            .append_pair("comp", "block")
            .append_pair(
                "blockid",
                &base64::engine::general_purpose::STANDARD.encode(descriptor.block_id.as_bytes()),
            );

        let response = context
            .storage_client
            .put(block_url)
            .header(CONTENT_LENGTH, descriptor.length)
            .body(Body::wrap_stream(stream))
            .send()
            .await?;
        ensure_success(
            response,
            format!("Azure block {} upload failed", descriptor.block_id),
        )
        .await?;
        Ok(())
    }

    async fn upload_via_azure(
        &self,
        init: &IngestionInit,
        file_path: &Path,
        file_name: &str,
        total_size: u64,
        concurrency: usize,
    ) -> Result<()> {
        use azure_storage_blobs::prelude::{BlobBlockType, BlobClient, BlockList};

        const AZURE_BLOCK_SIZE: u64 = 64 * 1024 * 1024;

        let url = init
            .presigned_url
            .as_deref()
            .ok_or_else(|| anyhow!("azure upload mode without presigned_url"))?;
        let sas_url = url.parse()?;
        let blob_client = BlobClient::from_sas_url(&sas_url)?;

        let concurrency = concurrency.max(1);
        let descriptors = azure_block_descriptors(total_size, AZURE_BLOCK_SIZE);
        let bar = Self::upload_progress_bar(file_name, total_size)?;
        let context = AzureBlockUploadContext {
            storage_client: self.storage_client.clone(),
            sas_url: Arc::new(sas_url.clone()),
            file_path: file_path.to_path_buf(),
            uploaded: Arc::new(AtomicU64::new(0)),
            bar: bar.clone(),
        };
        let cursor = Arc::new(Mutex::new(descriptors.clone().into_iter()));

        let workers = (0..concurrency).map(|_| {
            let context = context.clone();
            let cursor = Arc::clone(&cursor);
            async move {
                let mut file = tokio::fs::File::open(&context.file_path).await?;
                loop {
                    let descriptor = {
                        let mut cursor = cursor.lock().await;
                        cursor.next()
                    };
                    let Some(descriptor) = descriptor else {
                        return Ok::<_, anyhow::Error>(());
                    };

                    Self::put_block(&mut file, &context, descriptor).await?;
                }
            }
        });
        futures_util::future::try_join_all(workers).await?;

        let block_list = BlockList {
            blocks: descriptors
                .into_iter()
                .map(|descriptor| BlobBlockType::new_uncommitted(descriptor.block_id))
                .collect(),
        };
        blob_client.put_block_list(block_list).await?;
        bar.finish_and_clear();
        Ok(())
    }

    async fn put_part(
        file: &mut tokio::fs::File,
        context: &MultipartUploadContext,
        part: PartUrl,
    ) -> Result<()> {
        let offset = u64::from(part.part_number - 1) * context.part_size;
        if offset >= context.total_size {
            return Err(anyhow!(
                "part {} offset is outside the file",
                part.part_number
            ));
        }
        let part_len = context.part_size.min(context.total_size - offset);

        file.seek(SeekFrom::Start(offset)).await?;
        let mut data = vec![0; usize::try_from(part_len)?];
        file.read_exact(&mut data).await?;

        let stream =
            progress_reporting_stream(data, Arc::clone(&context.uploaded), context.bar.clone());

        let response = context
            .storage_client
            .put(part.url)
            .header(CONTENT_LENGTH, part_len)
            .body(Body::wrap_stream(stream))
            .send()
            .await?;
        ensure_success(
            response,
            format!("part {} storage PUT failed", part.part_number),
        )
        .await?;
        Ok(())
    }

    fn signed_parts_stream(
        &self,
        ingestion_id: i32,
        batch_size: usize,
    ) -> impl futures_util::Stream<Item = Result<PartUrl>> + '_ {
        async_stream::try_stream! {
            let mut next_part = Some(1);

            while let Some(start_part) = next_part {
                let urls = self.get_part_urls(ingestion_id, start_part, batch_size).await?;
                if urls.parts.is_empty() {
                    Err(anyhow!("server returned no multipart upload URLs"))?;
                }

                for part in urls.parts {
                    yield part;
                }

                next_part = urls.next_part;
            }
        }
    }

    async fn upload_via_multipart(
        &self,
        init: &IngestionInit,
        file_path: &Path,
        file_name: &str,
        total_size: u64,
        concurrency: usize,
    ) -> Result<()> {
        let part_size = init
            .part_size
            .ok_or_else(|| anyhow!("multipart upload mode without part_size"))?;
        if part_size == 0 {
            return Err(anyhow!("multipart upload part_size must be positive"));
        }
        let concurrency = concurrency.max(1);

        let bar = Self::upload_progress_bar(file_name, total_size)?;

        let uploaded = Arc::new(AtomicU64::new(0));
        let batch_size = concurrency.max(32);
        let context = MultipartUploadContext {
            storage_client: self.storage_client.clone(),
            file_path: file_path.to_path_buf(),
            part_size,
            total_size,
            uploaded,
            bar: bar.clone(),
        };
        let parts = self.signed_parts_stream(init.ingestion_id, batch_size);
        let parts = Arc::new(Mutex::new(Box::pin(parts)));

        let workers = (0..concurrency).map(|_| {
            let context = context.clone();
            let parts = Arc::clone(&parts);
            async move {
                let mut file = tokio::fs::File::open(&context.file_path).await?;
                loop {
                    let part = {
                        let mut parts = parts.lock().await;
                        parts.next().await.transpose()?
                    };
                    let Some(part) = part else {
                        return Ok::<_, anyhow::Error>(());
                    };

                    Self::put_part(&mut file, &context, part).await?;
                }
            }
        });
        futures_util::future::try_join_all(workers).await?;

        bar.finish_and_clear();
        Ok(())
    }

    async fn push_file(
        &self,
        stream_id: i32,
        metadata: &HashMap<String, Value>,
        file_path: &Path,
        concurrency: usize,
        upload_mode: UploadModeOverride,
    ) -> Result<i32> {
        let file_name = file_path.file_name().unwrap().to_string_lossy().to_string();
        let total_size = tokio::fs::metadata(file_path).await?.len();

        let init = self
            .init_ingestion(stream_id, &file_name, total_size, metadata)
            .await?;

        let upload_result = async {
            match (upload_mode, &init.mode) {
                (UploadModeOverride::Server, _) | (_, UploadMode::Server) => {
                    self.upload_via_server(&init, file_path, &file_name, total_size)
                        .await?;
                }
                (_, UploadMode::Azure) => {
                    self.upload_via_azure(&init, file_path, &file_name, total_size, concurrency)
                        .await?;
                }
                (_, UploadMode::Single) => {
                    self.upload_via_single(&init, file_path, &file_name, total_size)
                        .await?;
                }
                (_, UploadMode::Multipart) => {
                    self.upload_via_multipart(
                        &init,
                        file_path,
                        &file_name,
                        total_size,
                        concurrency,
                    )
                    .await?;
                }
            }
            self.complete_upload(init.ingestion_id).await?;
            Ok::<_, anyhow::Error>(init.dataset_id)
        }
        .await;

        match upload_result {
            Ok(dataset_id) => Ok(dataset_id),
            Err(e) => {
                let _ = self
                    .abort_upload(init.ingestion_id, &format!("{:#}", e))
                    .await;
                Err(e)
            }
        }
    }

    async fn download_dataset(
        &self,
        dataset: &Dataset,
        output_dir: &Option<String>,
    ) -> Result<String> {
        let Some(backup_size) = dataset.backup_size else {
            return Err(anyhow!("Dataset {} has no backup", dataset.id));
        };
        let endpoint = format!(
            "stream/{}/dataset/{}/backup",
            dataset.datastream_id, dataset.id
        );
        let BackupResponse { path } = self.get_json(&endpoint, &()).await?;
        let local_path =
            Path::new(&output_dir.clone().unwrap_or(".".to_string())).join(dataset.path.clone());
        let mut file = tokio::fs::File::create(local_path.clone()).await?;
        let mut bytes_stream = reqwest::get(path).await?.bytes_stream();
        let bar = progress_bar(&dataset.path, backup_size)?;
        let mut downloaded = 0;
        while let Some(chunk) = bytes_stream.next().await {
            if let Ok(chunk) = &chunk {
                file.write_all(chunk).await?;
                downloaded += chunk.len() as u64;
                bar.set_position(downloaded);
            }
        }
        bar.finish_and_clear();
        Ok(local_path.to_string_lossy().to_string())
    }
}

async fn handle_ping(marpledb: &MarpleDB) -> Result<()> {
    match marpledb.health().await {
        Ok(health) if health.status == "healthy" => {
            println!("{} MarpleDB API is healthy", "✓".green());
            Ok(())
        }
        Ok(_) => {
            println!("{} MarpleDB API is not healthy", "✗".red());
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("{} MarpleDB API is not healthy: {}", "✗".red(), e);
            std::process::exit(1);
        }
    }
}

fn handle_version() {
    println!("mdb {}", env!("CARGO_PKG_VERSION"));
}

async fn handle_stream_commands(marpledb: &MarpleDB, command: &StreamCommands) -> Result<()> {
    match command {
        StreamCommands::List => {
            let streams = marpledb.get_streams().await?;
            println!("{}", serde_json::to_string_pretty(&streams)?);
        }
        StreamCommands::Get { stream_name } => {
            let stream = marpledb.get_stream(stream_name).await?;
            println!("{}", serde_json::to_string_pretty(&stream)?);
        }
        StreamCommands::New {
            stream_name,
            properties,
        } => {
            marpledb.create_stream(stream_name, properties).await?;
            let new_stream = marpledb.get_stream(stream_name).await?;
            println!("{}", serde_json::to_string_pretty(&new_stream)?);
        }
        StreamCommands::Update {
            stream_name,
            properties,
        } => {
            let stream = marpledb.get_stream(stream_name).await?;
            marpledb.update_stream(stream.id, properties).await?;
            let updated_stream = marpledb.get_stream(stream_name).await?;
            println!("{}", serde_json::to_string_pretty(&updated_stream)?);
        }
    }
    Ok(())
}

async fn handle_dataset_commands(
    marpledb: &MarpleDB,
    stream_name: &str,
    command: &DatasetCommands,
) -> Result<()> {
    let stream = marpledb.get_stream(stream_name).await?;

    match command {
        DatasetCommands::List => {
            let datasets = marpledb.get_datasets(stream.id).await?;
            println!("{}", serde_json::to_string_pretty(&datasets)?);
        }
        DatasetCommands::Get { dataset_id } => {
            let dataset = marpledb.get_dataset(stream.id, *dataset_id).await?;
            println!("{}", serde_json::to_string_pretty(&dataset)?);
        }
        DatasetCommands::Download {
            dataset_id,
            output_dir,
        } => {
            if let Some(dataset_id) = dataset_id {
                let dataset = marpledb.get_dataset(stream.id, *dataset_id).await?;
                match marpledb.download_dataset(&dataset, output_dir).await {
                    Ok(path) => println!("{} {} -> {}", "✓".green(), dataset.path, path),
                    Err(e) => eprintln!("{} {} failed: {}", "✗".red(), dataset.id, e),
                }
            } else {
                let datasets = marpledb.get_datasets(stream.id).await?;
                for dataset in datasets {
                    match marpledb.download_dataset(&dataset, output_dir).await {
                        Ok(path) => println!("{} {} -> {}", "✓".green(), dataset.path, path),
                        Err(e) => eprintln!("{} {} failed: {}", "✗".red(), dataset.id, e),
                    }
                }
            }
        }
    }
    Ok(())
}

struct IngestOptions<'a> {
    recursive: bool,
    extension: Option<&'a str>,
    skip_existing: bool,
    concurrency: usize,
    upload_mode: UploadModeOverride,
}

async fn handle_ingest(
    marpledb: &MarpleDB,
    stream_name: &str,
    metadata: &HashMap<String, Value>,
    files: &[PathBuf],
    options: IngestOptions<'_>,
) -> Result<()> {
    let stream = marpledb.get_stream(stream_name).await?;
    let existing: HashSet<String> = if options.skip_existing {
        marpledb
            .get_datasets(stream.id)
            .await?
            .into_iter()
            .map(|dataset| dataset.path)
            .collect()
    } else {
        HashSet::new()
    };
    let should_ingest = |path: &Path| -> bool {
        path.is_file()
            && options.extension.is_none_or(|ext| {
                path.extension().is_some_and(|path_ext| {
                    path_ext.to_string_lossy().to_ascii_lowercase()
                        == ext.to_ascii_lowercase().trim_start_matches('.')
                })
            })
    };

    for path in files {
        if should_ingest(path) {
            ingest_path(marpledb, stream.id, &existing, metadata, &options, path).await;
        } else if options.recursive && path.is_dir() {
            for entry in WalkDir::new(path)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
            {
                let file_path = entry.path();
                if should_ingest(file_path) {
                    ingest_path(
                        marpledb, stream.id, &existing, metadata, &options, file_path,
                    )
                    .await;
                }
            }
        } else {
            println!("{} {} skipped", "-".yellow(), path.display());
        }
    }
    Ok(())
}

async fn ingest_path(
    marpledb: &MarpleDB,
    stream_id: i32,
    existing: &HashSet<String>,
    metadata: &HashMap<String, Value>,
    options: &IngestOptions<'_>,
    path: &Path,
) {
    let file_name = path.file_name().unwrap().to_string_lossy().to_string();
    if existing.contains(&file_name) {
        println!(
            "{} {} - already exists, skipping...",
            "-".yellow(),
            file_name
        );
        return;
    }
    match marpledb
        .push_file(
            stream_id,
            metadata,
            path,
            options.concurrency,
            options.upload_mode,
        )
        .await
    {
        Ok(dataset_id) => println!("{} {} {}", "✓".green(), file_name, dataset_id),
        Err(e) => println!("{} {} failed: {}", "✗".red(), file_name, e),
    }
}

async fn handle_get(
    marpledb: &MarpleDB,
    endpoint: &str,
    params: Vec<(String, Value)>,
) -> Result<()> {
    let result = marpledb.get(endpoint, Some(params)).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn handle_post(
    marpledb: &MarpleDB,
    endpoint: &str,
    data: Vec<(String, Value)>,
) -> Result<()> {
    let result = marpledb.post(endpoint, Some(data)).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn handle_delete(
    marpledb: &MarpleDB,
    endpoint: &str,
    data: Vec<(String, Value)>,
) -> Result<()> {
    let result = marpledb.delete(endpoint, Some(data)).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let marpledb = MarpleDB::new(&cli.mdb_url, &cli.mdb_token)?;

    let Some(command) = cli.command else {
        if cli.version {
            handle_version();
        } else {
            Cli::command().print_help()?;
        }
        return Ok(());
    };

    // Check health
    if marpledb.health().await.is_err() {
        eprintln!("{} {} is not responding", "✗".red(), cli.mdb_url);
        std::process::exit(1);
    }

    // Check token
    if marpledb.get_streams().await.is_err() {
        eprintln!("{} Invalid token", "✗".red());
        std::process::exit(1);
    }

    match command {
        Commands::Ping => handle_ping(&marpledb).await?,
        Commands::Stream { command } => handle_stream_commands(&marpledb, &command).await?,
        Commands::Dataset {
            stream_name,
            command,
        } => handle_dataset_commands(&marpledb, &stream_name, &command).await?,
        Commands::Ingest {
            stream_name,
            metadata,
            files,
            recursive,
            extension,
            skip_existing,
            concurrency,
            upload_mode,
        } => {
            let metadata = to_record(metadata);
            handle_ingest(
                &marpledb,
                &stream_name,
                &metadata,
                &files,
                IngestOptions {
                    recursive,
                    extension: extension.as_deref(),
                    skip_existing,
                    concurrency,
                    upload_mode,
                },
            )
            .await?
        }
        Commands::Get { endpoint, params } => handle_get(&marpledb, &endpoint, params).await?,
        Commands::Post { endpoint, data } => handle_post(&marpledb, &endpoint, data).await?,
        Commands::Delete { endpoint, data } => handle_delete(&marpledb, &endpoint, data).await?,
    }

    Ok(())
}
