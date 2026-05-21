# marple-db

Rust client SDK for the MarpleDB API. The crate is published as `marple-db` and imported from Rust code as `marple_db`.

## Installation

The SDK is async and works with any runtime supported by `reqwest`. The examples below use Tokio:

```toml
[dependencies]
marple-db = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
anyhow = "1"
serde_json = "1"
```

## Authentication

Create an API token in the MarpleDB web application and pass it to the SDK:

```sh
export MDB_TOKEN="mdb_your_token_here"
export MDB_URL="https://db.marpledata.com/api/v1"
```

`MDB_URL` is optional for your own code, but the URL passed to `MarpleDB::new` should point at the API root and usually ends in `/api/v1`.

## Quickstart

This example uploads `run.csv` to an existing stream named `runs`, waits for import to finish, and prints the final status.

```rust
use marple_db::{ImportStatus, MarpleDB, PushFileOptions};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("MDB_URL")
        .unwrap_or_else(|_| "https://db.marpledata.com/api/v1".to_string());
    let token = std::env::var("MDB_TOKEN")?;
    let db = MarpleDB::new(&url, &token)?;
    let stream = db.get_stream("runs").await?;

    let dataset = db
        .push_file(
            stream.id,
            "run.csv",
            PushFileOptions::builder()
                .metadata([("source", json!("example"))])
                .build(),
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
- `db.get_streams()` lists streams.
- `db.get_stream("runs")` finds a stream by name.
- `db.create_stream("runs", &serde_json::json!({ "plugin": "csv" }))` creates a stream.
- `db.update_stream(stream_id, &serde_json::json!({ ... }))` updates stream metadata.
- `db.get_datasets(stream_id)` lists datasets in a stream.
- `db.get_dataset(stream_id, dataset_id)` fetches one dataset.
- `db.push_file(stream_id, path, PushFileOptions::default())` uploads a file.
- `db.wait_for_import(stream_id, dataset_id, timeout)` polls until import reaches a terminal status.
- `db.get_download_link(&dataset)` returns a pre-signed URL for the original uploaded file.
- `db.get`, `db.post`, and `db.delete` call API endpoints that do not have typed helpers yet.

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

let options = PushFileOptions::builder()
    .metadata([
        ("driver", json!("Mbaerto")),
        ("run", json!(42)),
    ])
    .concurrency(8)
    .upload_mode(UploadModeOverride::Server)
    .build();
```

`UploadModeOverride::Server` forces uploads through the MarpleDB API server. Leave the default `Auto` unless you need that behavior. `concurrency` is used by multipart/direct-storage upload modes; higher values can improve throughput but use more memory and network connections.

For progress reporting, implement `ProgressReporter` and pass it through `PushFileOptions::builder().progress(...)`.

## Features

By default, the SDK keeps dependencies light and uses raw `reqwest` requests for the final Azure block-list commit.

Enable `azure-sdk-commit` to use `azure_storage_blobs` for that final commit step:

```toml
marple-db = { version = "0.1", features = ["azure-sdk-commit"] }
```

The `mdb` CLI enables this feature so CLI uploads use the Azure SDK-backed commit path. Block staging still uses raw `reqwest` in both modes to preserve the SDK's progress reporting and concurrency behavior.

## Downloading Original Files

`get_download_link` returns a pre-signed storage URL. The URL is already authenticated, so use `db.storage_client()` or another client that does not add MarpleDB authorization headers.

```rust
let url = db.get_download_link(&dataset).await?;
let bytes = db.storage_client().get(url).send().await?.bytes().await?;
std::fs::write(&dataset.path, bytes)?;
```

## Custom Clients

Use `MarpleDB::builder()` when you need custom timeouts, user agents, or preconfigured `reqwest::Client` instances:

```rust
use marple_db::MarpleDB;
use std::time::Duration;

let db = MarpleDB::builder()
    .url("https://db.marpledata.com/api/v1")
    .token("mdb_your_token_here")
    .timeout(Duration::from_secs(120))
    .user_agent("my-ingester/1.0")
    .build()?;
```

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

For HTTP-like failures, `error.status()` returns the API or storage status code when one is available.

## Tracing

The SDK emits `tracing` spans/events for API calls and upload mode dispatch. It does not install a tracing subscriber; applications should configure their own subscriber if they want logs or spans.

## Links

- Documentation: [docs.marpledata.com](https://docs.marpledata.com/docs)
- Repository: [github.com/marpledata/marple-sdk](https://github.com/marpledata/marple-sdk)
- Issues: [github.com/marpledata/marple-sdk/issues](https://github.com/marpledata/marple-sdk/issues)
- License: Apache-2.0 OR MIT
