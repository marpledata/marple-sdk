use marple_db::{MarpleDB, PushFileOptions};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url =
        std::env::var("MDB_URL").unwrap_or_else(|_| "https://db.marpledata.com/api/v1".to_string());
    let token = std::env::var("MDB_TOKEN")?;
    let db = MarpleDB::new(&url, &token)?;
    let stream = db.get_stream("runs").await?;

    let dataset = db
        .push_file(
            stream.id,
            "run.csv",
            PushFileOptions::builder()
                .metadata([("source", json!("rust-example"))])
                .build(),
        )
        .await?;
    let dataset = db
        .wait_for_import(stream.id, dataset.id, Duration::from_secs(180))
        .await?;

    println!(
        "uploaded dataset {} with status {:?}",
        dataset.id, dataset.import_status
    );
    Ok(())
}
