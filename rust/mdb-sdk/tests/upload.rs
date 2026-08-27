use marple_db::{Error, MarpleDB, PushFileOptions, UploadModeOverride};
use serde_json::json;
use std::fs;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn csv_file() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("run.csv");
    fs::write(&path, b"time,value\n0,1\n").expect("csv");
    (dir, path)
}

fn client(server: &MockServer) -> MarpleDB {
    MarpleDB::new(&format!("{}/api/v1", server.uri()), "mdb_test_token").expect("client")
}

fn ingestion_init(mode: &str) -> serde_json::Value {
    json!({
        "dataset_id": 42,
        "ingestion_id": 10,
        "mode": mode,
        "part_size": 8,
        "expires_in": 3600
    })
}

fn dataset_body() -> serde_json::Value {
    json!({
        "id": 42,
        "datastream_id": 7,
        "path": "run.csv",
        "import_status": "UPLOADING"
    })
}

#[tokio::test]
async fn server_upload_inits_uploads_completes_and_reloads_dataset() {
    let server = MockServer::start().await;
    let (_dir, file_path) = csv_file();

    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ingestion_init("server")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion/10/upload/server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion/10/upload/complete"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/stream/7/dataset/42"))
        .respond_with(ResponseTemplate::new(200).set_body_json(dataset_body()))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion/10/abort"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(0)
        .mount(&server)
        .await;

    let dataset = client(&server)
        .push_file(7, &file_path, PushFileOptions::default())
        .await
        .expect("upload");
    assert_eq!(dataset.id, 42);
    assert_eq!(dataset.datastream_id, 7);
}

#[tokio::test]
async fn failed_upload_aborts_and_skips_complete() {
    let server = MockServer::start().await;
    let (_dir, file_path) = csv_file();

    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ingestion_init("server")))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion/10/upload/server"))
        .respond_with(ResponseTemplate::new(500).set_body_string("disk full"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion/10/abort"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion/10/upload/complete"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(0)
        .mount(&server)
        .await;

    let error = client(&server)
        .push_file(7, &file_path, PushFileOptions::default())
        .await
        .expect_err("upload should fail");
    assert!(matches!(error, Error::Api { status: 500, .. }));
}

#[tokio::test]
async fn server_override_ignores_multipart_mode() {
    let server = MockServer::start().await;
    let (_dir, file_path) = csv_file();

    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ingestion_init("multipart")))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion/10/upload/server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/ingestion/10/upload/part-urls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "parts": [],
            "expires_in": 60
        })))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion/10/upload/complete"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/stream/7/dataset/42"))
        .respond_with(ResponseTemplate::new(200).set_body_json(dataset_body()))
        .mount(&server)
        .await;

    client(&server)
        .push_file(
            7,
            &file_path,
            PushFileOptions::default().upload_mode(UploadModeOverride::Server),
        )
        .await
        .expect("forced server upload");
}

#[tokio::test]
async fn path_without_file_name_is_a_config_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/ingestion"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ingestion_init("server")))
        .expect(0)
        .mount(&server)
        .await;

    let error = client(&server)
        .push_file(7, "/", PushFileOptions::default())
        .await
        .expect_err("root path");
    assert!(matches!(error, Error::Config(_)));
}
