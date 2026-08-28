# marple-db

Rust client SDK for the MarpleDB API. The crate is published as `marple-db` and imported from Rust code as `marple_db`.

## Installation

The SDK is async and **requires Tokio**. It does not install a runtime; use `#[tokio::main]` or another Tokio executor. Minimum supported Rust is **1.85**.

```toml
[dependencies]
marple-db = "0.3"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
anyhow = "1"
serde_json = "1"
```

Default TLS is `rustls-tls-native-roots` (rustls plus the OS certificate store). `rustls-tls` trusts Mozilla's CA bundle only; `native-tls` uses SChannel / Secure Transport / OpenSSL:

```toml
marple-db = { version = "0.3", default-features = false, features = ["rustls-tls"] }
```

## Authentication

Create an API token in the MarpleDB web application and pass it to the SDK. `SAAS_URL` is `https://db.marpledata.com/api/v1`; pass a different URL for VPC or self-hosted deployments.

```sh
export MDB_TOKEN="mdb_your_token_here"
# export MDB_URL="https://db.marpledata.com/api/v1"  # optional; defaults to SaaS
```

## Quickstart

This example uploads `run.csv` to an existing stream named `runs`, waits for import to finish, and prints the final status.

```rust
use marple_db::{ImportStatus, MarpleDB, PushFileOptions, SAAS_URL};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("MDB_URL").unwrap_or_else(|_| SAAS_URL.to_string());
    let token = std::env::var("MDB_TOKEN")?;
    let db = MarpleDB::new(&url, &token)?;
    let stream = db.get_stream("runs").await?;

    let dataset = db
        .push_file(
            stream.id,
            "run.csv",
            PushFileOptions::default()
                .metadata([("source", json!("example"))]),
        )
        .await?;

    let dataset = db
        .wait_for_import(stream.id, dataset.id, Duration::from_secs(180))
        .await?;

    if dataset.import_status == ImportStatus::Finished {
        println!("uploaded dataset {}", dataset.id);
    }
    Ok(())
}
```

## Common Operations

- `db.health()` checks the API health endpoint.
- `db.get_user_info()` fetches the authenticated user profile and workspace memberships.
- `db.get_current_workspace()` resolves the connected workspace name, license, and latest storage usage.
- `db.get_workspace_license()` fetches the license for the workspace bound to the token.
- `db.get_usage_series(UsageType::ColdStorage, None, None)` fetches a workspace usage series.
- `db.get_streams()` lists streams.
- `db.get_stream("runs")` finds a stream by name.
- `db.get_stream_by_id(stream_id)` fetches a stream by id.
- `db.create_stream("runs", &serde_json::json!({ "plugin": "csv" }))` creates a stream.
- `db.update_stream(stream_id, &serde_json::json!({ ... }))` updates stream metadata.
- `db.delete_stream(stream_id)` deletes a stream and all of its datasets (admin token required).
- `db.get_datasets(stream_id)` lists datasets in a stream.
- `db.get_metadata_fields(stream_id)` lists metadata keys used in a stream.
- `db.get_settings()` fetches workspace settings, including `INSIGHT_URL`.
- `db.get_datapool_datasets("default")` lists datasets across a datapool.
- `db.get_datapool_ingest_queue("default")` lists datasets currently in the ingest queue.
- `db.get_dataset(stream_id, dataset_id)` fetches one dataset.
- `db.get_dataset_by_path("default", "run.csv")` fetches a dataset by path within a datapool.
- `db.delete_dataset(stream_id, dataset_id)` deletes a dataset.
- `db.reingest_dataset(stream_id, dataset_id)` re-queues a dataset for ingest from its original file.
- `db.get_debug_messages(stream_id, dataset_id)` fetches ingest debug messages.
- `db.get_dataset_statuses(stream_id, &[dataset_id])` fetches import status for selected datasets.
- `db.get_signals(stream_id, dataset_id)` lists signals in a dataset.
- `db.push_file(stream_id, path, PushFileOptions::default())` uploads a file.
- `db.wait_for_import(stream_id, dataset_id, timeout)` polls until import reaches a terminal status.
- `db.download_original(&dataset, ".")` downloads the original uploaded file into a directory.
- `db.get_download_link(&dataset)` returns a pre-signed URL for the original uploaded file.
- `db.get`, `db.post`, `db.patch`, and `db.delete` call API endpoints that do not have typed helpers yet.

Generic endpoint helpers deserialize into the type you ask for:

```rust
let response: serde_json::Value = db
    .post("/query", &serde_json::json!({
        "query": "select path, stream_id from mdb_default_dataset limit 1"
    }))
    .await?;
```

Use `&()` when a GET request has no query parameters:

```rust
let value: serde_json::Value = db.get("/health", &()).await?;
```

## Upload Options

`push_file` asks the server which upload mode to use and automatically handles direct storage uploads, multipart uploads, Azure block uploads, and API-server uploads.

```rust
use marple_db::{PushFileOptions, UploadModeOverride};
use serde_json::json;

let options = PushFileOptions::default()
    .metadata([
        ("driver", json!("Mbaerto")),
        ("run", json!(42)),
    ])
    .dataset_name("heat1.csv")
    .concurrency(8)
    .upload_mode(UploadModeOverride::Server);
```

`dataset_name` becomes the dataset path in Marple DB and defaults to the local file name. `overwrite(true)` replaces an existing dataset with that same path. `UploadModeOverride::Server` forces uploads through the MarpleDB API server; leave the default `Auto` unless you need that behavior. `concurrency` is used by multipart/direct-storage upload modes; higher values can improve throughput but use more memory and network connections.

For progress reporting, implement `ProgressReporter` and pass it through `PushFileOptions::default().progress(...)`.

## Downloading Original Files

`download_original` fetches the original uploaded file into a directory. The filename comes from the dataset path.

```rust
let path = db.download_original(&dataset, ".").await?;
```

`get_download_link` still returns the pre-signed URL if you want to fetch it yourself. The URL is already authenticated; do not send MarpleDB authorization headers to it.

## Custom Clients

Use `MarpleDB::builder()` for custom timeouts or a user agent. For custom TLS (extra CA, `danger_accept_invalid_certs`, …), pass `reqwest::Client` instances. Those builder methods follow reqwest's semver; the rest of the SDK does not.

```rust
use marple_db::MarpleDB;
use std::time::Duration;

let http = reqwest::Client::builder()
    .timeout(Duration::from_secs(120))
    .build()?;
let db = MarpleDB::builder()
    .url(marple_db::SAAS_URL)
    .token("mdb_your_token_here")
    .user_agent("my-ingester/1.0")
    .client(http.clone())
    .storage_client(http)
    .build()?;
```

The storage client is used for pre-signed URLs and should not send MarpleDB authorization headers.

## Error Handling

The SDK returns `marple_db::Error`, which can be matched directly:

```rust
match db.get_stream("runs").await {
    Ok(stream) => println!("stream id: {}", stream.id),
    Err(marple_db::Error::StreamNotFound { name }) => {
        eprintln!("stream {name:?} does not exist");
    }
    Err(marple_db::Error::Api { status, body, .. }) => {
        eprintln!("MarpleDB returned {status}: {body}");
    }
    Err(error) => return Err(error.into()),
}
```

For HTTP-like failures, `error.status()` returns the API or storage status code (`u16`) when one is available.

## Timeouts and retries

SDK-built clients match the Python SDK: 5s connect / 300s total for API calls, 1800s for storage, with retries on the same methods and status codes. `MarpleDBBuilder::timeout` overrides the total timeout on both SDK-built clients. Streamed upload bodies are not retried.

The crate version is `marple_db::VERSION`.

## Tracing

The SDK emits `tracing` spans/events for API calls and upload mode dispatch. It does not install a tracing subscriber; applications should configure their own subscriber if they want logs or spans.

## Links

- Documentation: [docs.rs/marple-db](https://docs.rs/marple-db) · [docs.marpledata.com](https://docs.marpledata.com/docs)
- Repository: [github.com/marpledata/marple-sdk](https://github.com/marpledata/marple-sdk)
- Issues: [github.com/marpledata/marple-sdk/issues](https://github.com/marpledata/marple-sdk/issues)
- License: Apache-2.0
