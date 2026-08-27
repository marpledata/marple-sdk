use marple_db::{
    Dataset, ImportStatus, MarpleDB, Metadata, PushFileOptions, Stream, UploadModeOverride,
};
use serde_json::{Value, json};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_MDB_URL: &str = "https://db.marpledata.com/api/v1";
const TEST_STREAM_PREFIX: &str = "Salty Compulsory RustSdkTest";
const MIB: u64 = 1024 * 1024;
const MULTIPART_THRESHOLD: u64 = 128 * MIB;

static INTEGRATION_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
static SWEPT_LEFTOVER_STREAMS: AtomicBool = AtomicBool::new(false);

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
    Ok(MarpleDB::new(url, token)?)
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

fn tiny_csv_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test_data/tiny_race.csv")
        .canonicalize()
        .expect("tiny CSV path")
}

async fn cleanup_streams(db: &MarpleDB) -> anyhow::Result<()> {
    for stream in db.get_streams().await? {
        if !stream.name.starts_with(TEST_STREAM_PREFIX) {
            continue;
        }
        let _ = db
            .post::<_, Value>(&format!("/stream/{}/delete", stream.id), &json!({}))
            .await;
    }
    Ok(())
}

async fn sweep_leftover_streams(db: &MarpleDB) -> anyhow::Result<()> {
    if SWEPT_LEFTOVER_STREAMS.swap(true, Ordering::SeqCst) {
        return Ok(());
    }
    cleanup_streams(db).await
}

async fn create_test_stream(db: &MarpleDB, suffix: &str) -> anyhow::Result<Stream> {
    let options: Metadata = [("plugin_args".to_string(), json!("--use-index"))]
        .into_iter()
        .collect();
    Ok(db
        .create_stream(&unique_stream_name(suffix), &options)
        .await?)
}

async fn delete_stream(db: &MarpleDB, stream_id: i32) {
    let _ = db
        .post::<_, Value>(&format!("/stream/{}/delete", stream_id), &json!({}))
        .await;
}

async fn run_with_cleanup<F, Fut>(db: &MarpleDB, suffix: &str, flow: F) -> anyhow::Result<()>
where
    F: FnOnce(Stream) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let _guard = integration_test_guard().await;
    sweep_leftover_streams(db).await?;

    let stream = create_test_stream(db, suffix).await?;
    let stream_id = stream.id;
    let result = flow(stream).await;
    delete_stream(db, stream_id).await;
    result
}

async fn upload_and_assert_dataset(
    db: &MarpleDB,
    stream_id: i32,
    file_path: &Path,
    options: PushFileOptions,
    expected_metadata: &[(&str, &str)],
    exercise_generic_endpoints: bool,
    wait_for_import: bool,
) -> anyhow::Result<Dataset> {
    anyhow::ensure!(file_path.exists(), "test file missing at {:?}", file_path);
    let expected_size = fs::metadata(file_path)?.len();
    anyhow::ensure!(expected_size > 0, "test file is empty");

    let dataset = db.push_file(stream_id, file_path, options).await?;
    let dataset = if wait_for_import {
        let dataset = db
            .wait_for_import(stream_id, dataset.id, Duration::from_secs(180))
            .await?;
        anyhow::ensure!(
            dataset.import_status == ImportStatus::Finished,
            "ingest failed"
        );
        dataset
    } else {
        dataset
    };

    for (key, value) in expected_metadata {
        anyhow::ensure!(
            dataset.metadata.get(*key).and_then(Value::as_str) == Some(*value),
            "dataset metadata missing expected {key} value"
        );
    }

    if wait_for_import || dataset.backup_size.is_some() {
        let backup_size = dataset
            .backup_size
            .ok_or_else(|| anyhow::anyhow!("dataset has no backup_size"))?;
        anyhow::ensure!(
            backup_size == expected_size,
            "backup_size mismatch: source file is {expected_size} bytes, backup_size is {backup_size} bytes"
        );
    }

    anyhow::ensure!(
        db.get_datasets(stream_id)
            .await?
            .iter()
            .any(|candidate| candidate.id == dataset.id),
        "dataset id not found in dataset list"
    );

    if !wait_for_import {
        return Ok(dataset);
    }

    if exercise_generic_endpoints {
        let query = "select path, stream_id, metadata from mdb_default_dataset limit 1;";
        db.post::<_, Value>("/query", &json!({ "query": query }))
            .await?;

        let signals: Value = db
            .get(
                &format!("/stream/{}/dataset/{}/signals", stream_id, dataset.id),
                &(),
            )
            .await?;
        anyhow::ensure!(
            signals
                .as_array()
                .is_some_and(|signals| !signals.is_empty()),
            "signals response should be a non-empty array"
        );
    }

    let backup_size = dataset
        .backup_size
        .ok_or_else(|| anyhow::anyhow!("finished dataset has no backup_size"))?;
    let download_url = db.get_download_link(&dataset).await?;
    let response = db.storage_client().get(download_url).send().await?;
    anyhow::ensure!(
        response.status().is_success(),
        "download URL returned status {}",
        response.status()
    );
    let downloaded = response.bytes().await?;
    let downloaded_size = downloaded.len() as u64;
    anyhow::ensure!(
        downloaded_size == expected_size,
        "downloaded file size mismatch: source file is {expected_size} bytes, downloaded file is {downloaded_size} bytes"
    );
    anyhow::ensure!(
        downloaded_size == backup_size,
        "downloaded file size mismatch: backup_size is {backup_size} bytes, downloaded file is {downloaded_size} bytes"
    );

    Ok(dataset)
}

fn generate_multipart_blob(output_dir: &Path) -> anyhow::Result<PathBuf> {
    let output_path = output_dir.join("multipart-blob.bin");
    let mut output = fs::File::create(&output_path)?;
    output.write_all(b"multipart-upload-test")?;
    output.set_len(MULTIPART_THRESHOLD + 1)?;
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
    run_with_cleanup(&db, "auto", |stream| {
        let db = db.clone();
        async move { run_auto_upload_flow(&db, stream).await }
    })
    .await
}

#[tokio::test]
async fn test_sdk_server_upload_flow() -> anyhow::Result<()> {
    let Some((token, url)) = maybe_skip_integration() else {
        eprintln!("Skipping Rust SDK integration test: missing env var MDB_TOKEN");
        return Ok(());
    };

    let db = db(&token, &url)?;
    run_with_cleanup(&db, "server", |stream| {
        let db = db.clone();
        async move { run_server_upload_flow(&db, stream).await }
    })
    .await
}

#[tokio::test]
async fn test_sdk_overwrite_flow() -> anyhow::Result<()> {
    let Some((token, url)) = maybe_skip_integration() else {
        eprintln!("Skipping Rust SDK integration test: missing env var MDB_TOKEN");
        return Ok(());
    };

    let db = db(&token, &url)?;
    run_with_cleanup(&db, "overwrite", |stream| {
        let db = db.clone();
        async move { run_overwrite_flow(&db, stream).await }
    })
    .await
}

#[tokio::test]
async fn test_sdk_multipart_upload_flow() -> anyhow::Result<()> {
    let Some((token, url)) = maybe_skip_integration() else {
        eprintln!("Skipping Rust SDK integration test: missing env var MDB_TOKEN");
        return Ok(());
    };

    let db = db(&token, &url)?;
    run_with_cleanup(&db, "multipart", |stream| {
        let db = db.clone();
        async move { run_multipart_upload_flow(&db, stream).await }
    })
    .await
}

async fn run_auto_upload_flow(db: &MarpleDB, stream: Stream) -> anyhow::Result<()> {
    let csv_path = tiny_csv_path();

    let fetched = db.get_stream(&stream.name).await?;
    anyhow::ensure!(fetched.id == stream.id, "fetched stream id mismatch");

    anyhow::ensure!(
        db.get_streams()
            .await?
            .iter()
            .any(|candidate| candidate.id == stream.id),
        "created stream not found in stream list"
    );

    let update_options: Metadata = [("name".to_string(), json!(stream.name.clone()))]
        .into_iter()
        .collect();
    let updated = db.update_stream(stream.id, &update_options).await?;
    anyhow::ensure!(updated.id == stream.id, "updated stream id mismatch");

    let metadata_deployment = "integration-test";
    let metadata_foo = "Bar";
    upload_and_assert_dataset(
        db,
        stream.id,
        &csv_path,
        PushFileOptions::builder()
            .metadata([
                ("Deployment".to_string(), json!(metadata_deployment)),
                ("Foo".to_string(), json!(metadata_foo)),
            ])
            .build(),
        &[("Deployment", metadata_deployment), ("Foo", metadata_foo)],
        true,
        true,
    )
    .await?;

    Ok(())
}

async fn run_server_upload_flow(db: &MarpleDB, stream: Stream) -> anyhow::Result<()> {
    let csv_path = tiny_csv_path();
    let upload_mode = "server";

    upload_and_assert_dataset(
        db,
        stream.id,
        &csv_path,
        PushFileOptions::builder()
            .metadata([("upload_mode".to_string(), json!(upload_mode))])
            .upload_mode(UploadModeOverride::Server)
            .build(),
        &[("upload_mode", upload_mode)],
        false,
        true,
    )
    .await?;

    Ok(())
}

async fn run_overwrite_flow(db: &MarpleDB, stream: Stream) -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let csv_path = tmp.path().join("overwrite_test.csv");
    fs::copy(tiny_csv_path(), &csv_path)?;

    let dataset = upload_and_assert_dataset(
        db,
        stream.id,
        &csv_path,
        PushFileOptions::builder()
            .metadata([("version".to_string(), serde_json::json!("1"))])
            .build(),
        &[("version", "1")],
        false,
        true,
    )
    .await?;

    let dataset_overwritten = upload_and_assert_dataset(
        db,
        stream.id,
        &csv_path,
        PushFileOptions::builder()
            .metadata([("version".to_string(), serde_json::json!("2"))])
            .overwrite(true)
            .build(),
        &[("version", "2")],
        false,
        true,
    )
    .await?;

    anyhow::ensure!(
        dataset_overwritten.id == dataset.id,
        "overwrite created a new dataset instead of updating"
    );
    let datasets = db.get_datasets(stream.id).await?;
    anyhow::ensure!(
        datasets.len() == 1,
        "Expected exactly 1 dataset after overwrite, found {}",
        datasets.len()
    );

    Ok(())
}

async fn run_multipart_upload_flow(db: &MarpleDB, stream: Stream) -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let blob_path = generate_multipart_blob(tmp.path())?;
    let upload_mode = "multipart";

    upload_and_assert_dataset(
        db,
        stream.id,
        &blob_path,
        PushFileOptions::builder()
            .metadata([("upload_mode".to_string(), json!(upload_mode))])
            .build(),
        &[("upload_mode", upload_mode)],
        false,
        false,
    )
    .await?;

    Ok(())
}
