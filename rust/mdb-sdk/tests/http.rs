use marple_db::{HealthResponse, MarpleDB};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
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
async fn get_dataset_by_path_sends_query() {
    use marple_db::Dataset;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/datapool/default/dataset"))
        .and(query_param("path", "run.csv"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": 42,
            "datastream_id": 7,
            "path": "run.csv"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let dataset: Dataset = client(&server)
        .get_dataset_by_path("default", "run.csv")
        .await
        .expect("dataset");
    assert_eq!(dataset.id, 42);
    assert_eq!(dataset.path, "run.csv");
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
