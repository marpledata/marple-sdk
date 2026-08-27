use marple_db::{
    Dataset, ImportStatus, MarpleDB, Metadata, PushFileOptions, Stream, UploadModeOverride,
    UsageType,
};
use serde_json::{Value, json};
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    let url = env_opt("MDB_URL")?;
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

fn unique_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
}

fn unique_stream_name(suffix: &str) -> String {
    format!("{TEST_STREAM_PREFIX} {suffix} {}", unique_nanos())
}

fn example_csv_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test_data/examples_race.csv")
        .canonicalize()
        .expect("example CSV path")
}

fn staged_csv(label: &str) -> anyhow::Result<(tempfile::TempDir, PathBuf)> {
    let tmp = tempfile::tempdir()?;
    let path = tmp
        .path()
        .join(format!("rust-sdk-{label}-{}.csv", unique_nanos()));
    fs::copy(example_csv_path(), &path)?;
    Ok((tmp, path))
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

async fn delete_stream(db: &MarpleDB, stream_id: i64) {
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
    stream_id: i64,
    file_path: &Path,
    options: PushFileOptions,
    expected_metadata: &[(&str, &str)],
    exercise_generic_endpoints: bool,
) -> anyhow::Result<Dataset> {
    anyhow::ensure!(file_path.exists(), "test file missing at {:?}", file_path);
    let expected_size = fs::metadata(file_path)?.len();
    anyhow::ensure!(expected_size > 0, "test file is empty");

    let dataset = db.push_file(stream_id, file_path, options).await?;
    let dataset = db
        .wait_for_import(stream_id, dataset.id, Duration::from_secs(180))
        .await?;
    anyhow::ensure!(
        dataset.import_status == ImportStatus::Finished,
        "ingest failed"
    );

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
        "backup_size mismatch: source file is {expected_size} bytes, backup_size is {backup_size} bytes"
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
        db.post::<_, Value>("/query", &json!({ "query": query }))
            .await?;

        let signals = db.get_signals(stream_id, dataset.id).await?;
        anyhow::ensure!(
            !signals.is_empty(),
            "signals response should be a non-empty array"
        );
        anyhow::ensure!(
            signals.iter().all(|signal| {
                signal.datastream_id == Some(stream_id) && signal.dataset_id == Some(dataset.id)
            }),
            "signals should include the parent stream and dataset ids"
        );
    }

    let dir = tempfile::tempdir()?;
    let downloaded_path = db.download_original(&dataset, dir.path()).await?;
    let downloaded = fs::read(&downloaded_path)?;
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

fn generate_multipart_csv(output_dir: &Path) -> anyhow::Result<PathBuf> {
    let source = fs::read(example_csv_path())?;
    anyhow::ensure!(!source.is_empty(), "source CSV is empty");

    let output_path = output_dir.join(format!("rust-sdk-multipart-{}.csv", unique_nanos()));
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
async fn test_sdk_user_info_and_usage() -> anyhow::Result<()> {
    let _guard = integration_test_guard().await;
    let Some((token, url)) = maybe_skip_integration() else {
        eprintln!("Skipping Rust SDK integration test: missing env var MDB_TOKEN");
        return Ok(());
    };

    let db = db(&token, &url)?;
    let info = db.get_user_info().await?;
    anyhow::ensure!(
        info.current_workspace_id() == Some("staging"),
        "expected workspace staging, got {:?}",
        info.current_workspace_id()
    );

    db.get_workspace_license().await?;
    db.get_usage_series(UsageType::ColdStorage, None, None)
        .await?;
    db.get_settings().await?;

    let workspace = db.get_current_workspace().await?;
    anyhow::ensure!(
        workspace.id == "staging",
        "expected workspace staging, got {}",
        workspace.id
    );

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
    let (_tmp, csv_path) = staged_csv("auto")?;

    let fetched = db.get_stream(&stream.name).await?;
    anyhow::ensure!(fetched.id == stream.id, "fetched stream id mismatch");
    let fetched_by_id = db.get_stream_by_id(stream.id).await?;
    anyhow::ensure!(
        fetched_by_id.id == stream.id,
        "fetched stream-by-id mismatch"
    );
    db.get_metadata_fields(stream.id).await?;

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
        PushFileOptions::default().metadata([
            ("Deployment".to_string(), json!(metadata_deployment)),
            ("Foo".to_string(), json!(metadata_foo)),
        ]),
        &[("Deployment", metadata_deployment), ("Foo", metadata_foo)],
        true,
    )
    .await?;

    Ok(())
}

async fn run_server_upload_flow(db: &MarpleDB, stream: Stream) -> anyhow::Result<()> {
    let (_tmp, csv_path) = staged_csv("server")?;
    let upload_mode = "server";

    upload_and_assert_dataset(
        db,
        stream.id,
        &csv_path,
        PushFileOptions::default()
            .metadata([("upload_mode".to_string(), json!(upload_mode))])
            .upload_mode(UploadModeOverride::Server),
        &[("upload_mode", upload_mode)],
        false,
    )
    .await?;

    Ok(())
}

async fn run_overwrite_flow(db: &MarpleDB, stream: Stream) -> anyhow::Result<()> {
    let (_tmp, csv_path) = staged_csv("overwrite")?;

    let dataset = upload_and_assert_dataset(
        db,
        stream.id,
        &csv_path,
        PushFileOptions::default().metadata([("version".to_string(), serde_json::json!("1"))]),
        &[("version", "1")],
        false,
    )
    .await?;

    let dataset_overwritten = upload_and_assert_dataset(
        db,
        stream.id,
        &csv_path,
        PushFileOptions::default()
            .metadata([("version".to_string(), serde_json::json!("2"))])
            .overwrite(true),
        &[("version", "2")],
        false,
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
    let csv_path = generate_multipart_csv(tmp.path())?;
    let upload_mode = "multipart";

    upload_and_assert_dataset(
        db,
        stream.id,
        &csv_path,
        PushFileOptions::default().metadata([("upload_mode".to_string(), json!(upload_mode))]),
        &[("upload_mode", upload_mode)],
        false,
    )
    .await?;

    Ok(())
}
