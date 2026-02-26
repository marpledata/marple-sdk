use anyhow::{Result, anyhow};
use clap::{CommandFactory, Parser, Subcommand};
use colored::*;
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{
    Body, Client, Response,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
    multipart::{Form, Part},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use walkdir::WalkDir;

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

#[derive(Debug, Serialize, Deserialize)]
struct IngestResponse {
    dataset_id: i32,
}

struct MarpleDB {
    client: Client,
    base_url: String,
}

impl MarpleDB {
    fn new(url: &str, token: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        let mut bearer = HeaderValue::from_str(&format!("Bearer {}", token))?;
        bearer.set_sensitive(true);
        headers.insert(AUTHORIZATION, bearer);
        let client = Client::builder().default_headers(headers).build()?;

        Ok(Self {
            client,
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
            eprintln!("{} Stream {} not found", "✗".red(), stream_name);
            std::process::exit(1);
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
            .map(|d| serde_json::from_value(d.clone()).unwrap())
            .collect();
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

    async fn ingest_file(
        &self,
        stream_id: i32,
        datasets: &[Dataset],
        file_path: &Path,
    ) -> Result<IngestResponse> {
        let file_name = file_path.file_name().unwrap().to_string_lossy().to_string();
        for dataset in datasets {
            if dataset.path == file_name {
                println!(
                    "{} {} {} - already exists, skipping...",
                    "-".yellow(),
                    file_name,
                    dataset.id
                );
                return Ok(IngestResponse {
                    dataset_id: dataset.id,
                });
            }
        }

        let file = tokio::fs::File::open(file_path).await?;
        let total_size = file.metadata().await?.len();

        let mut uploaded = 0;
        let bar = ProgressBar::new(total_size);
        bar.set_style(ProgressStyle::default_bar().template(
            "- {msg} [{wide_bar}] ({binary_bytes_per_sec}, eta {eta}) {binary_bytes}/{binary_total_bytes}",
        )?.progress_chars("=> "));
        bar.set_message(file_name.clone());

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

        let mut form = Form::new();
        form = form.part("dataset_name", Part::text(file_name.clone()));
        form = form.part(
            "file",
            Part::stream_with_length(Body::wrap_stream(stream), total_size).file_name(file_name.clone()),
        );

        let endpoint = format!("stream/{}/ingest", stream_id);
        let mut request = self.client.post(self.base_url.clone() + &endpoint);
        request = request.multipart(form);
        let response = self
            .handle_response(&endpoint, "POST", request.send().await?)
            .await;
        match response {
            Ok(result) => {
                let result: IngestResponse = serde_json::from_value(result)?;
                println!("{} {} {}", "✓".green(), file_name, result.dataset_id);
                Ok(result)
            }
            Err(e) => {
                eprintln!("{} {} failed: {}", "✗".red(), file_name, e);
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
        let endpoint = &format!(
            "stream/{}/dataset/{}/backup",
            dataset.datastream_id, dataset.id
        );
        let BackupResponse { path }: BackupResponse =
            serde_json::from_value(self.get(&endpoint, None).await?)?;
        let local_path =
            Path::new(&output_dir.clone().unwrap_or(".".to_string())).join(dataset.path.clone());
        let mut file = tokio::fs::File::create(local_path.clone()).await?;
        let mut bytes_stream = reqwest::get(path).await?.bytes_stream();
        let bar = ProgressBar::new(backup_size);
        bar.set_style(ProgressStyle::default_bar().template(
            "- {msg} [{wide_bar}] ({binary_bytes_per_sec}, eta {eta}) {binary_bytes}/{binary_total_bytes}",
        )?.progress_chars("=> "));
        bar.set_message(dataset.path.clone());
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

async fn handle_ingest(
    marpledb: &MarpleDB,
    stream_name: &str,
    files: &[PathBuf],
    recursive: bool,
    extension: Option<&str>,
    skip_existing: bool,
) -> Result<()> {
    let stream = marpledb.get_stream(stream_name).await?;
    let should_ingest = |path: &Path| -> bool {
        path.is_file()
            && extension.is_none_or(|ext| {
                path.extension().is_some_and(|path_ext| {
                    path_ext.to_string_lossy().to_ascii_lowercase()
                        == ext.to_ascii_lowercase().trim_start_matches('.')
                })
            })
    };
    let existing = if skip_existing {
        marpledb.get_datasets(stream.id).await?
    } else {
        vec![]
    };
    for path in files {
        if should_ingest(path) {
            marpledb.ingest_file(stream.id, &existing, path).await?;
        } else if recursive && path.is_dir() {
            for entry in WalkDir::new(path)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
            {
                let file_path = entry.path();
                if should_ingest(file_path) {
                    marpledb
                        .ingest_file(stream.id, &existing, file_path)
                        .await?;
                }
            }
        } else {
            eprintln!("{} {} skipped", "✗".red(), path.display());
        }
    }
    Ok(())
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
            files,
            recursive,
            extension,
            skip_existing,
        } => {
            handle_ingest(
                &marpledb,
                &stream_name,
                &files,
                recursive,
                extension.as_deref(),
                skip_existing,
            )
            .await?
        }
        Commands::Get { endpoint, params } => handle_get(&marpledb, &endpoint, params).await?,
        Commands::Post { endpoint, data } => handle_post(&marpledb, &endpoint, data).await?,
        Commands::Delete { endpoint, data } => handle_delete(&marpledb, &endpoint, data).await?,
    }

    Ok(())
}
