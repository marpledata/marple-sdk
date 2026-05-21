# marple-db

Rust SDK for the MarpleDB API.

## Installation

```toml
[dependencies]
marple-db = "0.1"
```

## Example

```rust
use marple_db::{MarpleDB, NoopProgress, PushFileOptions, UploadModeOverride};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("MDB_URL")?;
    let token = std::env::var("MDB_TOKEN")?;
    let db = MarpleDB::new(&url, &token)?;
    let stream = db.get_stream("runs").await?;

    let dataset = db
        .push_file(
            stream.id,
            "run.csv",
            PushFileOptions {
                metadata: Default::default(),
                concurrency: 4,
                upload_mode: UploadModeOverride::Auto,
                progress: &NoopProgress,
            },
        )
        .await?;

    println!("uploaded dataset {}", dataset.id);
    Ok(())
}
```

See the [MarpleDB documentation](https://docs.marpledata.com/docs) for API details.
