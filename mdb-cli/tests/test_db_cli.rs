use assert_cmd::Command;
use assert_cmd::cargo::cargo_bin_cmd;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::Value;
use std::env;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn env_opt(name: &str) -> Option<String> {
    env::var(name).ok().filter(|s| !s.trim().is_empty())
}

fn maybe_skip_integration() -> Option<(String, Option<String>)> {
    let token = env_opt("MDB_TOKEN")?;
    let url = env_opt("MDB_URL");
    Some((token, url))
}

fn mdb_cmd(token: &str, url: Option<&str>) -> Command {
    let mut cmd = cargo_bin_cmd!("mdb");
    cmd.env("MDB_TOKEN", token);
    cmd.env("NO_COLOR", "1");
    if let Some(url) = url {
        cmd.env("MDB_URL", url);
    }
    cmd
}

fn parse_json_stdout(output: &assert_cmd::assert::Assert) -> Value {
    let out = output.get_output();
    let s = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(&s)
        .unwrap_or_else(|e| panic!("failed to parse JSON stdout: {e}\nstdout:\n{s}"))
}

fn unique_stream_name() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    format!("Salty Compulsory Rusttest {ts}")
}

fn find_last_i32(s: &str) -> Option<i32> {
    let bytes = s.as_bytes();
    let mut i = bytes.len();
    while i > 0 && !bytes[i - 1].is_ascii_digit() {
        i -= 1;
    }
    if i == 0 {
        return None;
    }
    let mut j = i;
    while j > 0 && bytes[j - 1].is_ascii_digit() {
        j -= 1;
    }
    s[j..i].parse::<i32>().ok()
}

fn example_csv_path() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    root.join("../python/tests/examples_race.csv")
}

async fn download_to_file(url: &str, dest: &Path) {
    let resp = reqwest::get(url).await.expect("download GET");
    assert!(
        resp.status().is_success(),
        "download failed: {}",
        resp.status()
    );
    let bytes = resp.bytes().await.expect("download bytes");
    let mut f = fs::File::create(dest).expect("create dest");
    f.write_all(&bytes).expect("write dest");
}

fn is_parquet_file(path: &Path) -> bool {
    let meta = match fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return false,
    };
    if meta.len() < 8 {
        return false;
    }

    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return false,
    };
    bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1")
}

fn verify_parquet_columns(path: &Path) {
    let file = File::open(path).expect("open parquet file");
    let reader = SerializedFileReader::new(file).expect("create parquet reader");
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();

    let column_names: Vec<String> = schema
        .columns()
        .iter()
        .map(|col| col.name().to_string())
        .collect();

    assert!(
        column_names.contains(&"time".to_string()),
        "parquet file missing 'time' column. Found columns: {:?}",
        column_names
    );
    assert!(
        column_names.contains(&"signal".to_string()),
        "parquet file missing 'signal' column. Found columns: {:?}",
        column_names
    );
    assert!(
        column_names.contains(&"value".to_string()),
        "parquet file missing 'value' column. Found columns: {:?}",
        column_names
    );
}

#[test]
fn test_version() {
    let mut cmd = cargo_bin_cmd!("mdb");
    cmd.env("NO_COLOR", "1");
    cmd.arg("--version");
    let output = cmd.assert().success();
    let stdout = String::from_utf8_lossy(&output.get_output().stdout);
    let stdout = stdout.trim();

    // Version output should be in format "mdb X.Y.Z" (possibly with pre-release suffix)
    assert!(
        stdout.starts_with("mdb "),
        "version output should start with 'mdb ', got: {}",
        stdout
    );

    // Extract version part
    let version_part = stdout.strip_prefix("mdb ").unwrap();

    // Split by '-' to separate version from pre-release identifier (e.g., "0.1.0-alpha.1")
    let (base_version, _) = version_part.split_once('-').unwrap_or((version_part, ""));

    // Verify base version has at least major.minor format
    let parts: Vec<&str> = base_version.split('.').collect();
    assert!(
        parts.len() >= 2,
        "version should have at least major.minor format, got: {}",
        base_version
    );

    // Verify major and minor parts are numeric
    assert!(
        parts[0].chars().all(|c| c.is_ascii_digit()),
        "major version should be numeric, got: {}",
        parts[0]
    );
    assert!(
        parts[1].chars().all(|c| c.is_ascii_digit()),
        "minor version should be numeric, got: {}",
        parts[1]
    );
}

#[tokio::test]
async fn test_db_ping_via_cli() {
    let Some((token, url)) = maybe_skip_integration() else {
        panic!("Skipping Rust CLI integration test: missing env var MDB_TOKEN");
    };

    mdb_cmd(&token, url.as_deref())
        .arg("ping")
        .assert()
        .success();
}

#[tokio::test]
async fn test_db_flow_via_cli() {
    let Some((token, url)) = maybe_skip_integration() else {
        panic!("Skipping Rust CLI integration test: missing env var MDB_TOKEN");
    };

    let stream_name = unique_stream_name();
    let csv_path = example_csv_path();
    assert!(csv_path.exists(), "example CSV missing at {:?}", csv_path);
    let csv_size = fs::metadata(&csv_path).expect("csv metadata").len();

    // Create stream
    let stream_json = mdb_cmd(&token, url.as_deref())
        .args(["stream", "new", &stream_name])
        .assert()
        .success();
    let stream_obj = parse_json_stdout(&stream_json);
    let stream_id = stream_obj
        .get("id")
        .and_then(|v| v.as_i64())
        .expect("stream id") as i32;

    // Ensure stream appears in list
    let streams_json = mdb_cmd(&token, url.as_deref())
        .args(["stream", "list"])
        .assert()
        .success();
    let streams = parse_json_stdout(&streams_json);
    let streams = streams.as_array().expect("streams should be array");
    assert!(
        streams
            .iter()
            .any(|s| s.get("name").and_then(|n| n.as_str()) == Some(&stream_name)),
        "stream not found in stream list"
    );

    // Ingest CSV -> parse dataset_id from stdout
    let ingest = mdb_cmd(&token, url.as_deref())
        .args(["ingest", &stream_name])
        .arg(&csv_path)
        .assert()
        .success();
    let ingest_out = String::from_utf8_lossy(&ingest.get_output().stdout).to_string();
    let dataset_id = find_last_i32(&ingest_out).expect("dataset_id from ingest output");

    // Poll ingest status until finished/failed
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut last_status = None::<String>;
    while std::time::Instant::now() < deadline {
        let ds_get = mdb_cmd(&token, url.as_deref())
            .args(["dataset", &stream_name, "get", &dataset_id.to_string()])
            .assert()
            .success();
        let ds_obj = parse_json_stdout(&ds_get);
        let status = ds_obj
            .get("import_status")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        last_status = Some(status.clone());
        if status == "FINISHED" || status == "FAILED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(
        last_status.as_deref(),
        Some("FINISHED"),
        "ingest did not finish"
    );

    // List datasets
    let ds_list = mdb_cmd(&token, url.as_deref())
        .args(["dataset", &stream_name, "list"])
        .assert()
        .success();
    let datasets = parse_json_stdout(&ds_list);
    let datasets = datasets.as_array().expect("datasets should be array");
    assert!(
        datasets
            .iter()
            .any(|d| d.get("id").and_then(|v| v.as_i64()) == Some(dataset_id as i64)),
        "dataset id not found in dataset list"
    );

    // Query endpoint
    let query = "select path, stream_id, metadata from mdb_default_dataset limit 1;";
    mdb_cmd(&token, url.as_deref())
        .args(["post", "/query", &format!("query={query}")])
        .assert()
        .success();

    // Download original/backup to temp dir, validate size
    let tmp = tempfile::tempdir().expect("tempdir");
    mdb_cmd(&token, url.as_deref())
        .args([
            "dataset",
            &stream_name,
            "download",
            "--output-dir",
            tmp.path().to_str().unwrap(),
            &dataset_id.to_string(),
        ])
        .assert()
        .success();
    let mut downloaded = None::<PathBuf>;
    for entry in fs::read_dir(tmp.path()).expect("read tmpdir") {
        let p = entry.expect("dir entry").path();
        if p.is_file() {
            downloaded = Some(p);
            break;
        }
    }
    let downloaded = downloaded.expect("downloaded file missing");
    let downloaded_size = fs::metadata(&downloaded)
        .expect("downloaded metadata")
        .len();
    assert_eq!(downloaded_size, csv_size, "downloaded file size mismatch");

    // Download a signal parquet (replicates python DB.download_signal checks)
    let signals_json = mdb_cmd(&token, url.as_deref())
        .args([
            "get",
            &format!("/stream/{stream_id}/dataset/{dataset_id}/signals"),
        ])
        .assert()
        .success();

    let signals = parse_json_stdout(&signals_json);
    let signals = signals.as_array().expect("signals should be array");
    let signal_id = signals
        .first()
        .and_then(|s| s.get("id"))
        .and_then(|v| v.as_i64())
        .expect("signal id") as i32;

    let paths_json = mdb_cmd(&token, url.as_deref())
        .args([
            "get",
            &format!("/datapool/default/dataset/{dataset_id}/signal/{signal_id}/data"),
        ])
        .assert()
        .success();
    let paths = parse_json_stdout(&paths_json).as_array()
        .expect("paths array")
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    assert!(!paths.is_empty(), "no parquet paths returned");

    let parquet_dir = tempfile::tempdir().expect("parquet tempdir");
    for (idx, url) in paths.iter().enumerate() {
        let p = parquet_dir
            .path()
            .join(format!("signal-{signal_id}-{idx}.parquet"));
        download_to_file(url, &p).await;
        assert!(
            is_parquet_file(&p),
            "downloaded signal is not a parquet file"
        );
        verify_parquet_columns(&p);
    }

    // Cleanup: delete stream (and datasets)
    mdb_cmd(&token, url.as_deref())
        .args(["post", &format!("/stream/{stream_id}/delete")])
        .assert()
        .success();
}
