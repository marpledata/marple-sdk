use anyhow::{Context, Result, anyhow};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use colored::*;
use futures_util::StreamExt;
use marple_db::{
    Dataset, MarpleDB, Metadata, ProgressReporter, PushFileOptions, UploadModeOverride,
};
use mdb_cli::{
    DatasetListFormat, IndicatifProgress, StreamListFormat, dataset_queue_table_header,
    dataset_table_header, format_dataset_queue_table_row, format_dataset_table_row,
    format_stream_table_row, progress_bar, progress_bar_or_hidden,
};
use serde_json::Value;
use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use walkdir::WalkDir;

#[derive(Parser)]
#[command(name = "mdb")]
#[command(about = "MarpleDB CLI - Interact with MarpleDB API")]
#[command()]
struct Cli {
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        help = "Load environment variables from a dotenv file before reading MDB_TOKEN and MDB_URL"
    )]
    env_file: Option<PathBuf>,

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

    /// Datapool commands
    Datapool {
        #[command(subcommand)]
        command: DatapoolCommands,
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
    List {
        /// Output format
        #[arg(long, value_enum, default_value_t = StreamListFormat::Short)]
        format: StreamListFormat,
    },

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
    List {
        /// Output format
        #[arg(long, value_enum, default_value_t = DatasetListFormat::Short)]
        format: DatasetListFormat,
    },

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

#[derive(Subcommand)]
enum DatapoolCommands {
    /// List datasets in a datapool
    Datasets {
        /// Datapool name
        #[arg(long, default_value = "default")]
        pool: String,

        /// Output format
        #[arg(long, value_enum, default_value_t = DatasetListFormat::Short)]
        format: DatasetListFormat,

        /// Show only datasets in the ingest queue
        #[arg(long)]
        queue: bool,
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

fn env_file_from_args<I>(args: I) -> Result<Option<PathBuf>>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--" {
            return Ok(None);
        }
        if arg == "--env-file" {
            let Some(path) = args.next() else {
                return Err(anyhow!("--env-file requires a path"));
            };
            return Ok(Some(PathBuf::from(path)));
        }
        if let Some(arg) = arg.to_str()
            && let Some(path) = arg.strip_prefix("--env-file=")
        {
            return Ok(Some(PathBuf::from(path)));
        }
    }
    Ok(None)
}

fn load_env() -> Result<()> {
    if let Some(path) = env_file_from_args(std::env::args_os())? {
        dotenvy::from_path(&path)
            .with_context(|| format!("failed to load env file {}", path.display()))?;
    } else {
        dotenvy::dotenv().ok();
    }
    Ok(())
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
        StreamCommands::List { format } => {
            let streams = marpledb.get_streams().await?;
            match format {
                StreamListFormat::Short => {
                    println!("{}", mdb_cli::stream_table_header());
                    for stream in streams {
                        println!("{}", format_stream_table_row(&stream));
                    }
                }
                StreamListFormat::Long => println!("{}", serde_json::to_string_pretty(&streams)?),
            }
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
        DatasetCommands::List { format } => {
            let datasets = marpledb.get_datasets(stream.id).await?;
            print_datasets(&datasets, *format)?;
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

async fn handle_datapool_commands(marpledb: &MarpleDB, command: &DatapoolCommands) -> Result<()> {
    match command {
        DatapoolCommands::Datasets {
            pool,
            format,
            queue,
        } => {
            let datasets = if *queue {
                marpledb.get_datapool_ingest_queue(pool).await?
            } else {
                marpledb.get_datapool_datasets(pool).await?
            };
            if *queue {
                print_dataset_queue(&datasets, *format)?;
            } else {
                print_datasets(&datasets, *format)?;
            }
        }
    }
    Ok(())
}

fn print_datasets(datasets: &[Dataset], format: DatasetListFormat) -> Result<()> {
    match format {
        DatasetListFormat::Short => {
            println!("{}", dataset_table_header());
            for dataset in datasets {
                println!("{}", format_dataset_table_row(dataset));
            }
        }
        DatasetListFormat::Long => println!("{}", serde_json::to_string_pretty(datasets)?),
    }
    Ok(())
}

fn print_dataset_queue(datasets: &[Dataset], format: DatasetListFormat) -> Result<()> {
    match format {
        DatasetListFormat::Short => {
            println!("{}", dataset_queue_table_header());
            for dataset in datasets {
                println!("{}", format_dataset_queue_table_row(dataset));
            }
        }
        DatasetListFormat::Long => println!("{}", serde_json::to_string_pretty(datasets)?),
    }
    Ok(())
}

fn download_progress(dataset: &marple_db::Dataset) -> IndicatifProgress {
    progress_bar_or_hidden(&dataset.path, dataset.backup_size)
}

async fn download_dataset(
    marpledb: &MarpleDB,
    dataset: &Dataset,
    output_dir: Option<&str>,
) -> Result<String> {
    let progress = download_progress(dataset);
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
        } else if !path.exists() {
            println!(
                "{} {} - file or folder not found",
                "✗".red(),
                path.display()
            );
        } else if path.is_dir() {
            println!(
                "{} {} - is a directory, use --recursive to ingest its contents",
                "-".yellow(),
                path.display()
            );
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
    load_env()?;
    let cli = Cli::parse();
    let marpledb = MarpleDB::builder()
        .url(&cli.mdb_url)
        .token(&cli.mdb_token)
        .request_source(concat!("cli/rust:", env!("CARGO_PKG_VERSION")))
        .build()?;

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
        Commands::Datapool { command } => handle_datapool_commands(&marpledb, &command).await?,
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
