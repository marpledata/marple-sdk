use marple_db::{Dataset, MarpleDB, Metadata, NoopProgress, PushFileOptions, UploadModeOverride};
use serde_json::{Value, json};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_MDB_URL: &str = "https://db.marpledata.com/api/v1";
const TEST_STREAM_PREFIX: &str = "Salty Compulsory RustSdkTest";
const MIB: u64 = 1024 * 1024;
const MULTIPART_THRESHOLD: u64 = 128 * MIB;

static INTEGRATION_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

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

async fn integration_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    INTEGRATION_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn unique_stream_name(suffix: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    format!("{TEST_STREAM_PREFIX} {suffix} {ts}")
}

fn example_csv_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test_data/examples_race.csv")
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

async fn create_test_stream(db: &MarpleDB, suffix: &str) -> anyhow::Result<marple_db::Stream> {
    let options: Metadata = [("plugin_args".to_string(), json!("--use-index"))].into();
    db.create_stream(&unique_stream_name(suffix), &options)
        .await
}

async fn run_with_cleanup<F, Fut>(db: &MarpleDB, flow: F) -> anyhow::Result<()>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let _guard = integration_test_guard().await;
    cleanup_streams(db).await?;

    let result = flow().await;
    let cleanup_result = cleanup_streams(db).await;

    if let Err(cleanup_error) = cleanup_result {
        if result.is_ok() {
            return Err(cleanup_error);
        }
        eprintln!("Rust SDK integration cleanup failed: {cleanup_error:#}");
    }

    result
}

async fn upload_and_assert_dataset(
    db: &MarpleDB,
    stream_id: i32,
    file_path: &Path,
    options: PushFileOptions,
    expected_metadata: &[(&str, &str)],
    exercise_generic_endpoints: bool,
) -> anyhow::Result<Dataset> {
    anyhow::ensure!(file_path.exists(), "test CSV missing at {:?}", file_path);
    let expected_size = fs::metadata(file_path)?.len();
    anyhow::ensure!(expected_size > 0, "test CSV is empty");

    let dataset = db.push_file(stream_id, file_path, options).await?;
    let dataset = wait_for_import(db, stream_id, dataset.id).await?;
    anyhow::ensure!(dataset.import_status == "FINISHED", "ingest failed");

    for (key, value) in expected_metadata {
        anyhow::ensure!(
            dataset.metadata.get(*key).and_then(Value::as_str) == Some(*value),
            "dataset metadata missing expected {key} value"
        );
    }

    let backup_size = dataset
        .backup_size
        .ok_or_else(|| anyhow::anyhow!("finished dataset has no backup_size"))?;
    anyhow::ensure!(
        backup_size == expected_size,
        "backup_size mismatch: source csv is {expected_size} bytes, backup_size is {backup_size} bytes"
    );

    anyhow::ensure!(
        db.get_datasets(stream_id)
            .await?
            .iter()
            .any(|candidate| candidate.id == dataset.id),
        "dataset id not found in dataset list"
    );

    if exercise_generic_endpoints {
        let query = "select path, stream_id, metadata from mdb_default_dataset limit 1;";
        db.post("/query", Some(vec![("query".to_string(), json!(query))]))
            .await?;

        let signals = db
            .get(
                &format!("/stream/{}/dataset/{}/signals", stream_id, dataset.id),
                None,
            )
            .await?;
        anyhow::ensure!(
            signals
                .as_array()
                .is_some_and(|signals| !signals.is_empty()),
            "signals response should be a non-empty array"
        );
    }

    let tmp = tempfile::tempdir()?;
    let downloaded = db
        .download_dataset(&dataset, Some(tmp.path()), &NoopProgress)
        .await?;
    let downloaded_size = fs::metadata(downloaded)?.len();
    anyhow::ensure!(
        downloaded_size == expected_size,
        "downloaded file size mismatch: source csv is {expected_size} bytes, downloaded file is {downloaded_size} bytes"
    );
    anyhow::ensure!(
        downloaded_size == backup_size,
        "downloaded file size mismatch: backup_size is {backup_size} bytes, downloaded file is {downloaded_size} bytes"
    );

    Ok(dataset)
}

fn generate_multipart_csv(output_dir: &Path) -> anyhow::Result<PathBuf> {
    let source = fs::read(example_csv_path())?;
    anyhow::ensure!(!source.is_empty(), "source CSV is empty");

    let output_path = output_dir.join("multipart-examples-race.csv");
    let repeat_count = (MULTIPART_THRESHOLD / source.len() as u64) + 1;
    let mut output = fs::File::create(&output_path)?;

    for _ in 0..repeat_count {
        output.write_all(&source)?;
    }
    output.flush()?;

    anyhow::ensure!(
        fs::metadata(&output_path)?.len() > MULTIPART_THRESHOLD,
        "generated multipart test file does not exceed multipart threshold"
    );
    Ok(output_path)
}

#[tokio::test]
async fn test_sdk_health_and_streams() -> anyhow::Result<()> {
    let _guard = integration_test_guard().await;
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
async fn test_sdk_auto_upload_flow() -> anyhow::Result<()> {
    let Some((token, url)) = maybe_skip_integration() else {
        eprintln!("Skipping Rust SDK integration test: missing env var MDB_TOKEN");
        return Ok(());
    };

    let db = db(&token, &url)?;
    run_with_cleanup(&db, || async { run_auto_upload_flow(&db).await }).await
}

#[tokio::test]
async fn test_sdk_server_upload_flow() -> anyhow::Result<()> {
    let Some((token, url)) = maybe_skip_integration() else {
        eprintln!("Skipping Rust SDK integration test: missing env var MDB_TOKEN");
        return Ok(());
    };

    let db = db(&token, &url)?;
    run_with_cleanup(&db, || async { run_server_upload_flow(&db).await }).await
}

#[tokio::test]
async fn test_sdk_multipart_upload_flow() -> anyhow::Result<()> {
    let Some((token, url)) = maybe_skip_integration() else {
        eprintln!("Skipping Rust SDK integration test: missing env var MDB_TOKEN");
        return Ok(());
    };

    let db = db(&token, &url)?;
    run_with_cleanup(&db, || async { run_multipart_upload_flow(&db).await }).await
}

async fn run_auto_upload_flow(db: &MarpleDB) -> anyhow::Result<()> {
    let csv_path = example_csv_path();
    let stream = create_test_stream(db, "auto").await?;

    let fetched = db.get_stream(&stream.name).await?;
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
            &[("name".to_string(), json!(stream.name.clone()))].into(),
        )
        .await?;
    anyhow::ensure!(updated.id == stream.id, "updated stream id mismatch");

    let metadata_deployment = "integration-test";
    let metadata_foo = "Bar";
    upload_and_assert_dataset(
        db,
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
        &[("Deployment", metadata_deployment), ("Foo", metadata_foo)],
        true,
    )
    .await?;

    Ok(())
}

async fn run_server_upload_flow(db: &MarpleDB) -> anyhow::Result<()> {
    let csv_path = example_csv_path();
    let stream = create_test_stream(db, "server").await?;
    let upload_mode = "server";

    upload_and_assert_dataset(
        db,
        stream.id,
        &csv_path,
        PushFileOptions {
            metadata: [("upload_mode".to_string(), json!(upload_mode))].into(),
            upload_mode: UploadModeOverride::Server,
            ..Default::default()
        },
        &[("upload_mode", upload_mode)],
        false,
    )
    .await?;

    Ok(())
}

async fn run_multipart_upload_flow(db: &MarpleDB) -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let csv_path = generate_multipart_csv(tmp.path())?;
    let stream = create_test_stream(db, "multipart").await?;
    let upload_mode = "multipart";

    upload_and_assert_dataset(
        db,
        stream.id,
        &csv_path,
        PushFileOptions {
            metadata: [("upload_mode".to_string(), json!(upload_mode))].into(),
            ..Default::default()
        },
        &[("upload_mode", upload_mode)],
        false,
    )
    .await?;

    Ok(())
}
