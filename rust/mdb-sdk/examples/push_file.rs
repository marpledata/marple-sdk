use marple_db::{MarpleDB, PushFileOptions, SAAS_URL};
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
                .metadata([("source", json!("rust-example"))])
                .overwrite(true),
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
