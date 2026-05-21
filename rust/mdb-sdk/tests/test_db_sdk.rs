use marple_db::{Dataset, MarpleDB, NoopProgress, PushFileOptions};
use serde_json::{Value, json};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_MDB_URL: &str = "https://db.marpledata.com/api/v1";
const TEST_STREAM_PREFIX: &str = "Salty Compulsory RustSdkTest";

fn load_env_files() {
    dotenvy::dotenv().ok();
    dotenvy::from_path("../../python/.env").ok();
}

fn env_opt(name: &str) -> Option<String> {
    env::var(name).ok().filter(|s| !s.trim().is_empty())
}

fn maybe_skip_integration() -> Option<(String, String)> {
    load_env_files();
    let token = env_opt("MDB_TOKEN")?;
    let url = env_opt("MDB_URL").unwrap_or_else(|| DEFAULT_MDB_URL.to_string());
    Some((token, url))
}

fn db(token: &str, url: &str) -> anyhow::Result<MarpleDB> {
    MarpleDB::new(url, token)
}

fn unique_stream_name() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    format!("{TEST_STREAM_PREFIX} {ts}")
}

fn example_csv_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../python/tests/examples_race.csv")
        .canonicalize()
        .expect("example CSV path")
}

async fn wait_for_import(
    db: &MarpleDB,
    stream_id: i32,
    dataset_id: i32,
) -> anyhow::Result<Dataset> {
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    let mut last_dataset = None;

    while std::time::Instant::now() < deadline {
        let dataset = db.get_dataset(stream_id, dataset_id).await?;
        if dataset.import_status == "FINISHED" || dataset.import_status == "FAILED" {
            return Ok(dataset);
        }
        last_dataset = Some(dataset);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let status = last_dataset
        .as_ref()
        .map(|dataset| dataset.import_status.as_str())
        .unwrap_or("unknown");
    anyhow::bail!("ingest did not finish before timeout, last status: {status}")
}

async fn cleanup_streams(db: &MarpleDB) -> anyhow::Result<()> {
    for stream in db.get_streams().await? {
        if !stream.name.starts_with(TEST_STREAM_PREFIX) {
            continue;
        }
        let _ = db
            .post(&format!("/stream/{}/delete", stream.id), None)
            .await;
    }
    Ok(())
}

#[tokio::test]
async fn test_sdk_health_and_streams() -> anyhow::Result<()> {
    let Some((token, url)) = maybe_skip_integration() else {
        eprintln!("Skipping Rust SDK integration test: missing env var MDB_TOKEN");
        return Ok(());
    };

    let db = db(&token, &url)?;
    assert_eq!(db.health().await?.status, "healthy");
    db.get_streams().await?;

    let invalid_db = MarpleDB::new(&url, "invalid_token")?;
    assert!(invalid_db.get_streams().await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_sdk_flow() -> anyhow::Result<()> {
    let Some((token, url)) = maybe_skip_integration() else {
        eprintln!("Skipping Rust SDK integration test: missing env var MDB_TOKEN");
        return Ok(());
    };

    let db = db(&token, &url)?;
    cleanup_streams(&db).await?;

    let result = run_sdk_flow(&db).await;
    let cleanup_result = cleanup_streams(&db).await;

    if let Err(cleanup_error) = cleanup_result {
        if result.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("Rust SDK integration cleanup failed: {cleanup_error:#}");
    }

    result
}

async fn run_sdk_flow(db: &MarpleDB) -> anyhow::Result<()> {
    let stream_name = unique_stream_name();
    let csv_path = example_csv_path();
    anyhow::ensure!(csv_path.exists(), "example CSV missing at {:?}", csv_path);
    let csv_size = fs::metadata(&csv_path)?.len();
    anyhow::ensure!(csv_size > 0, "source CSV is empty");

    let stream = db.create_stream(&stream_name, &Default::default()).await?;
    anyhow::ensure!(stream.name == stream_name, "created stream name mismatch");

    let fetched = db.get_stream(&stream_name).await?;
    anyhow::ensure!(fetched.id == stream.id, "fetched stream id mismatch");

    anyhow::ensure!(
        db.get_streams()
            .await?
            .iter()
            .any(|candidate| candidate.id == stream.id),
        "created stream not found in stream list"
    );

    let updated = db
        .update_stream(
            stream.id,
            &[("name".to_string(), json!(stream_name.clone()))].into(),
        )
        .await?;
    anyhow::ensure!(updated.id == stream.id, "updated stream id mismatch");

    let metadata_deployment = "integration-test";
    let metadata_foo = "Bar";
    let dataset = db
        .push_file(
            stream.id,
            &csv_path,
            PushFileOptions {
                metadata: [
                    ("Deployment".to_string(), json!(metadata_deployment)),
                    ("Foo".to_string(), json!(metadata_foo)),
                ]
                .into(),
                ..Default::default()
            },
        )
        .await?;

    let dataset = wait_for_import(db, stream.id, dataset.id).await?;
    anyhow::ensure!(dataset.import_status == "FINISHED", "ingest failed");
    anyhow::ensure!(
        dataset.metadata.get("Deployment").and_then(Value::as_str) == Some(metadata_deployment),
        "dataset metadata missing Deployment value"
    );
    anyhow::ensure!(
        dataset.metadata.get("Foo").and_then(Value::as_str) == Some(metadata_foo),
        "dataset metadata missing Foo value"
    );
    let backup_size = dataset
        .backup_size
        .ok_or_else(|| anyhow::anyhow!("finished dataset has no backup_size"))?;
    anyhow::ensure!(
        backup_size == csv_size,
        "backup_size mismatch: source csv is {csv_size} bytes, backup_size is {backup_size} bytes"
    );

    anyhow::ensure!(
        db.get_datasets(stream.id)
            .await?
            .iter()
            .any(|candidate| candidate.id == dataset.id),
        "dataset id not found in dataset list"
    );

    let query = "select path, stream_id, metadata from mdb_default_dataset limit 1;";
    db.post("/query", Some(vec![("query".to_string(), json!(query))]))
        .await?;

    let signals = db
        .get(
            &format!("/stream/{}/dataset/{}/signals", stream.id, dataset.id),
            None,
        )
        .await?;
    anyhow::ensure!(
        signals
            .as_array()
            .is_some_and(|signals| !signals.is_empty()),
        "signals response should be a non-empty array"
    );

    let tmp = tempfile::tempdir()?;
    let downloaded = db
        .download_dataset(&dataset, Some(tmp.path()), &NoopProgress)
        .await?;
    let downloaded_size = fs::metadata(downloaded)?.len();
    anyhow::ensure!(
        downloaded_size == csv_size,
        "downloaded file size mismatch: source csv is {csv_size} bytes, downloaded file is {downloaded_size} bytes"
    );
    anyhow::ensure!(
        downloaded_size == backup_size,
        "downloaded file size mismatch: backup_size is {backup_size} bytes, downloaded file is {downloaded_size} bytes"
    );

    Ok(())
}
