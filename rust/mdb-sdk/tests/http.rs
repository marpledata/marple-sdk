use marple_db::{HealthResponse, ImportStatus, MarpleDB};
use serde_json::json;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(server: &MockServer) -> MarpleDB {
    MarpleDB::new(&format!("{}/api/v1", server.uri()), "mdb_test_token").expect("client")
}

#[tokio::test]
async fn get_retries_service_unavailable_then_succeeds() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "healthy"})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(ResponseTemplate::new(503).set_body_string("busy"))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    let health: HealthResponse = client(&server).health().await.expect("health");
    assert_eq!(health.status, "healthy");
}

#[tokio::test]
async fn post_does_not_retry_service_unavailable() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v1/stream"))
        .respond_with(ResponseTemplate::new(503).set_body_string("busy"))
        .expect(1)
        .mount(&server)
        .await;

    let result = client(&server)
        .post::<_, serde_json::Value>("stream", &json!({"name": "runs"}))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn patch_sends_json_and_deserializes() {
    let server = MockServer::start().await;

    Mock::given(method("PATCH"))
        .and(path("/api/v1/stream/7"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .expect(1)
        .mount(&server)
        .await;

    let body: serde_json::Value = client(&server)
        .patch("stream/7", &json!({"description": "updated"}))
        .await
        .expect("patch");
    assert_eq!(body["ok"], true);
}

#[tokio::test]
async fn download_original_writes_backup_bytes() {
    use marple_db::Dataset;

    let server = MockServer::start().await;
    let body = b"csv-bytes";

    Mock::given(method("GET"))
        .and(path("/api/v1/stream/7/dataset/42/backup"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"path": format!("{}/file.bin", server.uri())})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/file.bin"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body.as_slice()))
        .mount(&server)
        .await;

    let dataset: Dataset = serde_json::from_value(json!({
        "id": 42,
        "datastream_id": 7,
        "path": "run.csv",
        "backup_size": body.len()
    }))
    .expect("dataset");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = client(&server)
        .download_original(&dataset, dir.path())
        .await
        .expect("download");
    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some("run.csv")
    );
    assert_eq!(std::fs::read(&path).expect("bytes"), body);
}

#[tokio::test]
async fn delete_stream_posts_empty_object() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/stream/7/delete"))
        .and(body_json(json!({})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "success"})))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .delete_stream(7)
        .await
        .expect("delete stream");
}

#[tokio::test]
async fn delete_dataset_posts_empty_object() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/stream/7/dataset/42/delete"))
        .and(body_json(json!({})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "success"})))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .delete_dataset(7, 42)
        .await
        .expect("delete dataset");
}

#[tokio::test]
async fn reingest_dataset_posts_empty_object() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/stream/7/dataset/42/reingest"))
        .and(body_json(json!({})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "success"})))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .reingest_dataset(7, 42)
        .await
        .expect("reingest");
}

#[tokio::test]
async fn get_debug_messages_returns_string_list() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/stream/7/dataset/42/debug"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!(["started", "done"])))
        .expect(1)
        .mount(&server)
        .await;

    let messages = client(&server)
        .get_debug_messages(7, 42)
        .await
        .expect("debug");
    assert_eq!(messages, ["started", "done"]);
}

#[tokio::test]
async fn get_dataset_statuses_posts_id_array() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/stream/7/datasets/status"))
        .and(body_json(json!([42])))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
            "dataset_id": 42,
            "import_status": "FINISHED",
            "import_progress": 1.0,
            "import_message": "done",
            "id": 99
        }])))
        .expect(1)
        .mount(&server)
        .await;

    let statuses = client(&server)
        .get_dataset_statuses(7, &[42])
        .await
        .expect("statuses");
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].dataset_id, 42);
    assert_eq!(statuses[0].import_status, ImportStatus::Finished);
    assert_eq!(statuses[0].import_progress, Some(1.0));
    assert_eq!(statuses[0].import_message.as_deref(), Some("done"));
    assert_eq!(statuses[0].extra.get("id"), Some(&json!(99)));
}

#[tokio::test]
async fn injected_http_clients_are_used() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/health"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"status": "healthy"})))
        .expect(1)
        .mount(&server)
        .await;

    let http = reqwest::Client::new();
    let health = MarpleDB::builder()
        .url(format!("{}/api/v1", server.uri()))
        .token("mdb_test_token")
        .client(http.clone())
        .storage_client(http)
        .build()
        .expect("client")
        .health()
        .await
        .expect("health");
    assert_eq!(health.status, "healthy");
}
