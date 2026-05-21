use anyhow::{Result, anyhow};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use colored::*;
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use marple_db::{
    Dataset, MarpleDB, Metadata, ProgressReporter, PushFileOptions, UploadModeOverride,
};
use serde_json::Value;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use walkdir::WalkDir;

fn progress_bar(message: &str, total_size: u64) -> Result<ProgressBar> {
    let bar = ProgressBar::new(total_size);
    bar.set_style(ProgressStyle::default_bar().template(
        "- {msg} [{wide_bar}] ({binary_bytes_per_sec}, eta {eta}) {binary_bytes}/{binary_total_bytes}",
    )?.progress_chars("=> "));
    bar.set_message(message.to_string());
    Ok(bar)
}

struct IndicatifProgress(ProgressBar);

impl ProgressReporter for IndicatifProgress {
    fn set_position(&self, position: u64) {
        self.0.set_position(position);
    }

    fn finish(&self) {
        self.0.finish_and_clear();
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
        env = "MDB_URL",
        help = "MarpleDB API URL; defaults to SaaS and usually ends in /api/v1"
    )]
    mdb_url: String,

    #[arg(
        long,
        default_value = "",
        env = "MDB_TOKEN",
        help = "MarpleDB API token; can also be provided through MDB_TOKEN"
    )]
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

        /// Dataset metadata as key=value pairs; values are parsed as JSON when possible
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

        /// Max concurrent direct-storage part uploads
        #[arg(long, default_value_t = 4)]
        concurrency: usize,

        /// Upload mode override; server forces upload through the API server
        #[arg(long, value_enum, default_value_t = CliUploadModeOverride::Auto)]
        upload_mode: CliUploadModeOverride,
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

        /// Query parameters as key=value pairs; values are parsed as JSON when possible
        #[arg(num_args = 0.., value_parser = parse_key_val)]
        params: Vec<(String, Value)>,
    },

    /// POST to a MarpleDB API endpoint
    Post {
        /// API endpoint
        endpoint: String,

        /// JSON body fields as key=value pairs; values are parsed as JSON when possible
        #[arg(num_args = 0.., value_parser = parse_key_val)]
        data: Vec<(String, Value)>,
    },

    /// DELETE a MarpleDB API endpoint
    Delete {
        /// API endpoint
        endpoint: String,

        /// JSON body fields as key=value pairs; values are parsed as JSON when possible
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

        /// Stream properties as key=value pairs; values are parsed as JSON when possible
        #[arg(num_args = 0.., value_parser = parse_key_val)]
        properties: Vec<(String, Value)>,
    },

    /// Update a stream
    Update {
        /// Stream name
        stream_name: String,

        /// Stream properties as key=value pairs; values are parsed as JSON when possible
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

    /// Download one dataset, or all datasets when no dataset id is provided
    Download {
        /// Output directory
        #[arg(short, long)]
        output_dir: Option<String>,

        /// Dataset ID; omit to download all datasets in the stream
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

fn to_record(pairs: Vec<(String, Value)>) -> Metadata {
    pairs.into_iter().collect()
}

#[derive(Clone, Copy, Debug, ValueEnum)]
#[clap(rename_all = "lowercase")]
enum CliUploadModeOverride {
    Auto,
    Server,
}

impl From<CliUploadModeOverride> for UploadModeOverride {
    fn from(value: CliUploadModeOverride) -> Self {
        match value {
            CliUploadModeOverride::Auto => Self::Auto,
            CliUploadModeOverride::Server => Self::Server,
        }
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
            let new_stream = marpledb
                .create_stream(stream_name, &to_record(properties.clone()))
                .await?;
            println!("{}", serde_json::to_string_pretty(&new_stream)?);
        }
        StreamCommands::Update {
            stream_name,
            properties,
        } => {
            let stream = marpledb.get_stream(stream_name).await?;
            let updated_stream = marpledb
                .update_stream(stream.id, &to_record(properties.clone()))
                .await?;
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
                match download_dataset(marpledb, &dataset, output_dir.as_deref()).await {
                    Ok(path) => println!("{} {} -> {}", "✓".green(), dataset.path, path),
                    Err(e) => eprintln!("{} {} failed: {}", "✗".red(), dataset.id, e),
                }
            } else {
                let datasets = marpledb.get_datasets(stream.id).await?;
                for dataset in datasets {
                    match download_dataset(marpledb, &dataset, output_dir.as_deref()).await {
                        Ok(path) => println!("{} {} -> {}", "✓".green(), dataset.path, path),
                        Err(e) => eprintln!("{} {} failed: {}", "✗".red(), dataset.id, e),
                    }
                }
            }
        }
    }
    Ok(())
}

fn download_progress(dataset: &marple_db::Dataset) -> Result<IndicatifProgress> {
    let bar = dataset
        .backup_size
        .map_or_else(ProgressBar::hidden, |size| {
            progress_bar(&dataset.path, size).unwrap_or_else(|_| ProgressBar::hidden())
        });
    Ok(IndicatifProgress(bar))
}

async fn download_dataset(
    marpledb: &MarpleDB,
    dataset: &Dataset,
    output_dir: Option<&str>,
) -> Result<String> {
    let progress = download_progress(dataset)?;
    let url = marpledb.get_download_link(dataset).await?;
    let local_path = output_dir
        .map_or_else(|| PathBuf::from("."), PathBuf::from)
        .join(&dataset.path);
    let mut file = tokio::fs::File::create(&local_path).await?;
    let response = marpledb.storage_client().get(url).send().await?;
    anyhow::ensure!(
        response.status().is_success(),
        "download failed with status {}",
        response.status()
    );
    let total = dataset.backup_size.unwrap_or_default();
    let mut downloaded = 0;
    let mut chunks = response.bytes_stream();

    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        progress.set_position(if total == 0 {
            downloaded
        } else {
            downloaded.min(total)
        });
    }
    progress.finish();
    Ok(local_path.to_string_lossy().to_string())
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
    metadata: &Metadata,
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
    metadata: &Metadata,
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

    let progress = match tokio::fs::metadata(path).await {
        Ok(metadata) => match progress_bar(&file_name, metadata.len()) {
            Ok(bar) => Arc::new(IndicatifProgress(bar)) as Arc<dyn ProgressReporter>,
            Err(e) => {
                println!("{} {} failed: {}", "✗".red(), file_name, e);
                return;
            }
        },
        Err(e) => {
            println!("{} {} failed: {}", "✗".red(), file_name, e);
            return;
        }
    };

    match marpledb
        .push_file(
            stream_id,
            path,
            PushFileOptions::builder()
                .metadata(metadata.clone())
                .concurrency(options.concurrency)
                .upload_mode(options.upload_mode)
                .progress(progress)
                .build(),
        )
        .await
    {
        Ok(dataset) => println!("{} {} {}", "✓".green(), file_name, dataset.id),
        Err(e) => println!("{} {} failed: {}", "✗".red(), file_name, e),
    }
}

async fn handle_get(
    marpledb: &MarpleDB,
    endpoint: &str,
    params: Vec<(String, Value)>,
) -> Result<()> {
    let result: Value = marpledb.get(endpoint, &to_record(params)).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn handle_post(
    marpledb: &MarpleDB,
    endpoint: &str,
    data: Vec<(String, Value)>,
) -> Result<()> {
    let result: Value = marpledb.post(endpoint, &to_record(data)).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn handle_delete(
    marpledb: &MarpleDB,
    endpoint: &str,
    data: Vec<(String, Value)>,
) -> Result<()> {
    let result: Value = marpledb.delete(endpoint, &to_record(data)).await?;
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
                    upload_mode: upload_mode.into(),
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
