use crate::errors::{Error, Result};
use crate::models::{
    CurrentWorkspace, Dataset, HealthResponse, ImportStatus, Settings, Signal, Stream, UsageSeries,
    UsageType, UserInfo, WorkspaceLicense,
};
use crate::progress::{NoopProgress, ProgressReporter};
use crate::retry::{self, API_RETRY, STORAGE_RETRY};
use futures_util::StreamExt;
use reqwest::{
    Client, Method, Response,
    header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue, USER_AGENT},
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use url::Url;

/// Identifies SDK-originated traffic in backend logs and metrics.
///
/// Sent on every API request via `X-Request-Source`. Matches the convention
/// used by the Python and MATLAB SDKs (`sdk/<lang>:<version>`). Callers can
/// override the value with [`MarpleDBBuilder::request_source`] to identify
/// higher-level tools built on top of the SDK.
const REQUEST_SOURCE_HEADER: HeaderName = HeaderName::from_static("x-request-source");
const DEFAULT_REQUEST_SOURCE: HeaderValue =
    HeaderValue::from_static(concat!("sdk/rust:", env!("CARGO_PKG_VERSION")));

/// Client for the MarpleDB API.
///
/// SDK-built HTTP clients use the Python SDK timeout defaults:
/// [`API_CONNECT_TIMEOUT`](Self::API_CONNECT_TIMEOUT) /
/// [`API_TIMEOUT`](Self::API_TIMEOUT) for API calls and
/// [`STORAGE_TIMEOUT`](Self::STORAGE_TIMEOUT) for pre-signed storage transfers.
///
/// This crate is async on Tokio. It does not install a runtime; call from
/// `#[tokio::main]` or another Tokio executor.
#[derive(Clone, Debug)]
pub struct MarpleDB {
    pub(crate) client: Client,
    pub(crate) storage_client: Client,
    pub(crate) base_url: String,
    auth_header: HeaderValue,
    request_source: HeaderValue,
}

impl MarpleDB {
    /// Connect timeout for SDK-built API clients (Python `API_TIMEOUT[0]`).
    pub const API_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
    /// Total request timeout for SDK-built API clients (Python `API_TIMEOUT[1]`).
    pub const API_TIMEOUT: Duration = Duration::from_secs(300);
    /// Connect and total timeout for SDK-built storage clients (Python `STORAGE_TIMEOUT`).
    pub const STORAGE_TIMEOUT: Duration = Duration::from_secs(1800);

    /// Creates a new client for `url` using a bearer API token.
    ///
    /// The URL should point at the MarpleDB API root and usually ends in
    /// `/api/v1`. Use [`crate::SAAS_URL`] for hosted Marple DB.
    pub fn new(url: &str, token: &str) -> Result<Self> {
        Self::builder().url(url).token(token).build()
    }

    /// Creates a builder for configuring a client.
    ///
    /// Use the builder when you need custom timeouts, a user agent, extra
    /// TLS roots, or to accept invalid certificates on a private network.
    #[must_use]
    pub fn builder() -> MarpleDBBuilder {
        MarpleDBBuilder::default()
    }

    /// Checks MarpleDB API health.
    pub async fn health(&self) -> Result<HealthResponse> {
        self.get("health", &()).await
    }

    /// Fetches workspace settings (`/settings`).
    ///
    /// The payload is a mix of Insight, storage-path, and build keys. Unknown
    /// keys are kept on [`Settings::extra`].
    pub async fn get_settings(&self) -> Result<Settings> {
        self.get("settings", &()).await
    }

    /// Lists metadata field names used by datasets in a stream.
    pub async fn get_metadata_fields(&self, stream_id: i64) -> Result<Vec<String>> {
        self.get(&format!("stream/{stream_id}/metadata/fields"), &())
            .await
    }

    /// Fetches the license for the workspace bound to this token.
    ///
    /// Returns `Ok(None)` when the API returns JSON `null` or HTTP 404. The
    /// top-level `workspace` field is the workspace slug, not the license row id.
    pub async fn get_workspace_license(&self) -> Result<Option<WorkspaceLicense>> {
        match self.get("workspace/license", &()).await {
            Err(Error::Api { status: 404, .. }) => Ok(None),
            result => result,
        }
    }

    /// Fetches the authenticated user profile, including workspace memberships.
    pub async fn get_user_info(&self) -> Result<UserInfo> {
        self.get("user/info", &()).await
    }

    /// Fetches a workspace usage series (`cold_storage`, `hot_storage`, …).
    pub async fn get_usage_series(
        &self,
        usage_type: UsageType,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<UsageSeries> {
        self.get(
            &format!("usage/series/{usage_type}"),
            &UsageQuery {
                start_time,
                end_time,
            },
        )
        .await
    }

    /// Resolves the current workspace name, license, and latest storage usage.
    ///
    /// Identity comes from `/user/info`. Usage points are best-effort.
    pub async fn get_current_workspace(&self) -> Result<CurrentWorkspace> {
        let (info, cold_bytes, hot_bytes, archive_bytes) = tokio::join!(
            self.get_user_info(),
            self.latest_usage(UsageType::ColdStorage),
            self.latest_usage(UsageType::HotStorage),
            self.latest_usage(UsageType::ArchiveStorage),
        );
        let info = info?;
        let id = info
            .current_workspace_id()
            .ok_or_else(|| {
                Error::Protocol("could not resolve current workspace from /user/info".to_string())
            })?
            .to_string();
        Ok(CurrentWorkspace {
            name: info.workspace_name(&id).to_string(),
            license: info.license,
            id,
            cold_bytes,
            hot_bytes,
            archive_bytes,
        })
    }

    /// Lists all streams visible to the token.
    pub async fn get_streams(&self) -> Result<Vec<Stream>> {
        Ok(self
            .get::<_, StreamsResponse>("streams", &())
            .await?
            .streams)
    }

    /// Finds a stream by name.
    pub async fn get_stream(&self, stream_name: &str) -> Result<Stream> {
        let streams = self.get_streams().await?;
        streams
            .into_iter()
            .find(|s| s.name == stream_name)
            .ok_or_else(|| Error::StreamNotFound {
                name: stream_name.to_string(),
            })
    }

    /// Fetches a stream by id.
    pub async fn get_stream_by_id(&self, stream_id: i64) -> Result<Stream> {
        match self.get(&format!("stream/{stream_id}"), &()).await {
            Err(Error::Api { status: 404, .. }) => Err(Error::StreamIdNotFound { id: stream_id }),
            result => result,
        }
    }

    /// Creates a stream with a name and serializable options object.
    ///
    /// `options` must serialize to a JSON object. The SDK adds the `name`
    /// field before sending the request.
    pub async fn create_stream<S: Serialize + ?Sized>(
        &self,
        stream_name: &str,
        options: &S,
    ) -> Result<Stream> {
        let mut options = match serde_json::to_value(options)? {
            Value::Object(options) => options,
            _ => {
                return Err(Error::Protocol(
                    "create_stream options must serialize to a JSON object".to_string(),
                ));
            }
        };
        options.insert("name".to_string(), Value::String(stream_name.to_string()));
        let response: CreatedStream = self.post("stream", &options).await?;
        self.get_stream_by_id(response.id).await
    }

    /// Updates a stream with a serializable options object.
    ///
    /// `options` must serialize to the JSON object expected by the MarpleDB
    /// stream update endpoint.
    pub async fn update_stream<S: Serialize + ?Sized>(
        &self,
        stream_id: i64,
        options: &S,
    ) -> Result<Stream> {
        let endpoint = format!("stream/update/{stream_id}");
        self.post::<_, Value>(&endpoint, options).await?;
        self.get_stream_by_id(stream_id).await
    }

    /// Lists datasets in a stream.
    pub async fn get_datasets(&self, stream_id: i64) -> Result<Vec<Dataset>> {
        self.get(&format!("stream/{}/datasets", stream_id), &())
            .await
    }

    /// Lists all datasets in a datapool.
    pub async fn get_datapool_datasets(&self, pool: &str) -> Result<Vec<Dataset>> {
        self.get(&format!("datapool/{}/datasets", pool), &()).await
    }

    /// Lists datasets currently in the ingest queue for a datapool.
    pub async fn get_datapool_ingest_queue(&self, pool: &str) -> Result<Vec<Dataset>> {
        self.get(&format!("datapool/{}/ingest/queue", pool), &())
            .await
    }

    /// Fetches a dataset by stream id and dataset id.
    pub async fn get_dataset(&self, stream_id: i64, dataset_id: i64) -> Result<Dataset> {
        self.get(&format!("stream/{}/dataset/{}", stream_id, dataset_id), &())
            .await
    }

    /// Lists signals in a dataset.
    pub async fn get_signals(&self, stream_id: i64, dataset_id: i64) -> Result<Vec<Signal>> {
        let mut signals: Vec<Signal> = self
            .get(
                &format!("stream/{stream_id}/dataset/{dataset_id}/signals"),
                &(),
            )
            .await?;
        for signal in &mut signals {
            signal.datastream_id = Some(stream_id);
            signal.dataset_id = Some(dataset_id);
        }
        Ok(signals)
    }

    /// Returns a pre-signed URL for downloading a dataset's original uploaded file.
    ///
    /// The returned URL is already authenticated and may expire. Prefer
    /// [`MarpleDB::download_original`] when you want the SDK to fetch the file.
    pub async fn get_download_link(&self, dataset: &Dataset) -> Result<Url> {
        if dataset.backup_size.is_none() {
            return Err(Error::NoBackup { id: dataset.id });
        }
        let endpoint = format!(
            "stream/{}/dataset/{}/backup",
            dataset.datastream_id, dataset.id
        );
        #[derive(serde::Deserialize)]
        struct DownloadLink {
            path: String,
        }
        let link: DownloadLink = self.get(&endpoint, &()).await?;
        Ok(link.path.parse()?)
    }

    /// Downloads the original uploaded file into `destination`.
    ///
    /// `destination` is a directory. The file is written as
    /// `destination / <dataset path filename>` and that path is returned.
    pub async fn download_original(
        &self,
        dataset: &Dataset,
        destination: impl AsRef<Path>,
    ) -> Result<PathBuf> {
        self.download_original_with_progress(dataset, destination, &NoopProgress)
            .await
    }

    /// Downloads the original uploaded file and reports byte progress.
    ///
    /// See [`MarpleDB::download_original`].
    pub async fn download_original_with_progress(
        &self,
        dataset: &Dataset,
        destination: impl AsRef<Path>,
        progress: &dyn ProgressReporter,
    ) -> Result<PathBuf> {
        let url = self.get_download_link(dataset).await?;
        let file_name = Path::new(&dataset.path)
            .file_name()
            .map(|name| name.to_os_string())
            .unwrap_or_else(|| format!("dataset-{}", dataset.id).into());
        let dest_dir = destination.as_ref();
        tokio::fs::create_dir_all(dest_dir).await?;
        let path = dest_dir.join(file_name);

        let response = retry::send_with_retry(
            self.storage_client.get(url.clone()),
            &Method::GET,
            &STORAGE_RETRY,
        )
        .await
        .map_err(|source| Error::storage("storage GET failed", None, None, Some(source)))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.map_err(|source| {
                Error::storage("storage GET failed", Some(status), None, Some(source))
            })?;
            return Err(Error::storage(
                "storage GET failed",
                Some(status),
                Some(body),
                None,
            ));
        }

        let mut file = tokio::fs::File::create(&path).await?;
        let mut downloaded = 0u64;
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|source| {
                Error::storage("storage GET failed", None, None, Some(source))
            })?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            progress.set_position(downloaded);
        }
        file.flush().await?;
        progress.finish();
        Ok(path)
    }

    /// Waits until an import reaches a terminal status or times out.
    ///
    /// Polls every 500ms. `Finished` and `Live` return the dataset, while
    /// `Failed`, `PostprocessingFailed`, and `CoolingFailed` return
    /// [`Error::ImportFailed`].
    pub async fn wait_for_import(
        &self,
        stream_id: i64,
        dataset_id: i64,
        timeout: Duration,
    ) -> Result<Dataset> {
        let deadline = std::time::Instant::now() + timeout;
        let mut last_status = "unknown".to_string();

        while std::time::Instant::now() < deadline {
            let dataset = self.get_dataset(stream_id, dataset_id).await?;
            last_status = format!("{:?}", dataset.import_status);

            match dataset.import_status {
                ImportStatus::Finished | ImportStatus::Live => return Ok(dataset),
                ImportStatus::Failed
                | ImportStatus::PostprocessingFailed
                | ImportStatus::CoolingFailed => {
                    return Err(Error::ImportFailed {
                        id: dataset.id,
                        message: dataset
                            .import_message
                            .clone()
                            .unwrap_or_else(|| format!("{:?}", dataset.import_status)),
                    });
                }
                _ => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }

        Err(Error::ImportTimeout {
            timeout_secs: timeout.as_secs(),
            last_status,
        })
    }

    /// Sends a GET request and deserializes the JSON response.
    ///
    /// Use `&()` for endpoints without query parameters. The response type is
    /// inferred from assignment or turbofish annotations.
    #[tracing::instrument(skip_all, fields(endpoint = %endpoint))]
    pub async fn get<Q, R>(&self, endpoint: &str, query: &Q) -> Result<R>
    where
        Q: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let request = self.auth(self.client.get(self.url(endpoint)).query(query));
        self.send_json(endpoint, Method::GET, request).await
    }

    /// Sends a POST request with a JSON body and deserializes the JSON response.
    ///
    /// The body may be any serializable value. Use `serde_json::Value` as the
    /// response type when calling untyped endpoints.
    #[tracing::instrument(skip_all, fields(endpoint = %endpoint))]
    pub async fn post<B, R>(&self, endpoint: &str, body: &B) -> Result<R>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let request = self.auth(self.client.post(self.url(endpoint)).json(body));
        self.send_json(endpoint, Method::POST, request).await
    }

    /// Sends a PATCH request with a JSON body and deserializes the JSON response.
    ///
    /// The body may be any serializable value. Use `serde_json::Value` as the
    /// response type when calling untyped endpoints.
    #[tracing::instrument(skip_all, fields(endpoint = %endpoint))]
    pub async fn patch<B, R>(&self, endpoint: &str, body: &B) -> Result<R>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let request = self.auth(self.client.patch(self.url(endpoint)).json(body));
        self.send_json(endpoint, Method::PATCH, request).await
    }

    /// Sends a DELETE request with a JSON body and deserializes the JSON response.
    ///
    /// The body may be any serializable value. Pass `&serde_json::json!({})`
    /// when the endpoint expects an empty JSON object.
    #[tracing::instrument(skip_all, fields(endpoint = %endpoint))]
    pub async fn delete<B, R>(&self, endpoint: &str, body: &B) -> Result<R>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let request = self.auth(self.client.delete(self.url(endpoint)).json(body));
        self.send_json(endpoint, Method::DELETE, request).await
    }

    fn url(&self, endpoint: &str) -> String {
        self.base_url.clone() + endpoint.trim_start_matches('/')
    }

    fn auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request
            .header(AUTHORIZATION, self.auth_header.clone())
            .header(REQUEST_SOURCE_HEADER, self.request_source.clone())
    }

    async fn send_json<R>(
        &self,
        endpoint: &str,
        method: Method,
        request: reqwest::RequestBuilder,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let response = retry::send_with_retry(request, &method, &API_RETRY)
            .await
            .map_err(|source| Error::transport(&method, endpoint, source))?;
        self.handle_response(endpoint, method, response).await
    }

    async fn handle_response<R>(
        &self,
        endpoint: &str,
        method: Method,
        response: Response,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| Error::transport(&method, endpoint, source))?;
        if !status.is_success() {
            return Err(Error::api(method, endpoint, status, body));
        }
        Ok(serde_json::from_str(&body)?)
    }

    #[tracing::instrument(skip_all, fields(endpoint = %endpoint))]
    pub(crate) async fn post_multipart(
        &self,
        endpoint: &str,
        form: reqwest::multipart::Form,
    ) -> Result<Value> {
        let request = self.auth(self.client.post(self.url(endpoint)).multipart(form));
        self.send_json(endpoint, Method::POST, request).await
    }

    async fn latest_usage(&self, usage_type: UsageType) -> Option<u64> {
        self.get_usage_series(usage_type, None, None)
            .await
            .ok()
            .and_then(|series| series.latest())
    }
}

/// Builder for `MarpleDB`.
#[must_use = "builder does nothing unless you call `.build()`"]
#[derive(Clone, Debug)]
pub struct MarpleDBBuilder {
    url: Option<String>,
    token: Option<String>,
    timeout: Option<Duration>,
    user_agent: Option<String>,
    request_source: Option<String>,
    danger_accept_invalid_certs: bool,
    root_certificates: Vec<Vec<u8>>,
}

impl Default for MarpleDBBuilder {
    fn default() -> Self {
        Self {
            url: None,
            token: None,
            timeout: None,
            user_agent: Some(format!("marple-db/{}", env!("CARGO_PKG_VERSION"))),
            request_source: None,
            danger_accept_invalid_certs: false,
            root_certificates: Vec::new(),
        }
    }
}

impl MarpleDBBuilder {
    /// Sets the MarpleDB API base URL.
    ///
    /// The URL should usually end in `/api/v1`. [`crate::SAAS_URL`] is the
    /// hosted default.
    #[must_use]
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Sets the bearer API token.
    ///
    /// The token is sent as `Authorization: Bearer <token>` on API requests.
    #[must_use]
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Sets the total timeout for the API and storage HTTP clients built by the SDK.
    ///
    /// Defaults match the Python SDK: 300s for API requests and 1800s for
    /// storage. This override applies the same total timeout to both clients.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the user agent for HTTP clients built by the SDK.
    #[must_use]
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Overrides the `X-Request-Source` header sent on every API request.
    ///
    /// The default is `sdk/rust:<crate-version>`. Higher-level tools built on
    /// top of the SDK should identify themselves so their traffic shows up
    /// distinctly in backend logs and metrics, for example `cli/rust:1.2.3`
    /// or `my-ingester/2.0.0`.
    #[must_use]
    pub fn request_source(mut self, request_source: impl Into<String>) -> Self {
        self.request_source = Some(request_source.into());
        self
    }

    /// Disables TLS certificate and hostname verification.
    ///
    /// Last resort for a private CA you cannot install. Prefer
    /// [`Self::add_root_certificate`] or the `native-tls` Cargo feature so
    /// the OS trust store is used. Applies to both API and storage clients.
    #[must_use]
    pub fn danger_accept_invalid_certs(mut self, accept_invalid: bool) -> Self {
        self.danger_accept_invalid_certs = accept_invalid;
        self
    }

    /// Trusts an additional PEM-encoded CA certificate.
    ///
    /// Use this for a known internal or self-signed server CA. Call multiple
    /// times to add more than one certificate. Applies to both API and
    /// storage clients.
    #[must_use]
    pub fn add_root_certificate(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.root_certificates.push(pem.into());
        self
    }

    /// Builds a configured `MarpleDB` client.
    pub fn build(self) -> Result<MarpleDB> {
        let url = self
            .url
            .ok_or_else(|| Error::Config("missing MarpleDB API URL".to_string()))?;
        let token = self
            .token
            .ok_or_else(|| Error::Config("missing MarpleDB API token".to_string()))?;
        let mut auth_header = header_value(&format!("Bearer {}", token))?;
        auth_header.set_sensitive(true);

        let request_source = match self.request_source {
            Some(value) => header_value(&value)?,
            None => DEFAULT_REQUEST_SOURCE,
        };

        let tls = TlsOptions {
            danger_accept_invalid_certs: self.danger_accept_invalid_certs,
            root_certificates: &self.root_certificates,
        };
        let api_timeout = self.timeout.unwrap_or(MarpleDB::API_TIMEOUT);
        let storage_timeout = self.timeout.unwrap_or(MarpleDB::STORAGE_TIMEOUT);
        let storage_connect = if self.timeout.is_some() {
            MarpleDB::API_CONNECT_TIMEOUT.min(storage_timeout)
        } else {
            MarpleDB::STORAGE_TIMEOUT
        };

        Ok(MarpleDB {
            client: build_client(
                api_timeout,
                MarpleDB::API_CONNECT_TIMEOUT.min(api_timeout),
                self.user_agent.as_deref(),
                tls,
            )?,
            storage_client: build_client(
                storage_timeout,
                storage_connect,
                self.user_agent.as_deref(),
                tls,
            )?,
            base_url: url.trim_end_matches('/').to_string() + "/",
            auth_header,
            request_source,
        })
    }
}

#[derive(Serialize)]
struct UsageQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    start_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_time: Option<i64>,
}

#[derive(Debug, serde::Deserialize)]
struct StreamsResponse {
    streams: Vec<Stream>,
}

#[derive(serde::Deserialize)]
struct CreatedStream {
    #[serde(deserialize_with = "crate::models::deserialize_i64")]
    id: i64,
}

fn header_value(value: &str) -> Result<HeaderValue> {
    HeaderValue::from_str(value)
        .map_err(|error| Error::Config(format!("invalid HTTP header value: {error}")))
}

#[derive(Clone, Copy)]
struct TlsOptions<'a> {
    danger_accept_invalid_certs: bool,
    root_certificates: &'a [Vec<u8>],
}

fn build_client(
    timeout: Duration,
    connect_timeout: Duration,
    user_agent: Option<&str>,
    tls: TlsOptions<'_>,
) -> Result<Client> {
    let mut builder = Client::builder()
        .timeout(timeout)
        .connect_timeout(connect_timeout)
        .danger_accept_invalid_certs(tls.danger_accept_invalid_certs);
    for pem in tls.root_certificates {
        let cert = reqwest::Certificate::from_pem(pem)
            .map_err(|error| Error::Config(format!("invalid root certificate: {error}")))?;
        builder = builder.add_root_certificate(cert);
    }
    if let Some(user_agent) = user_agent {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, header_value(user_agent)?);
        builder = builder.default_headers(headers);
    }
    builder
        .build()
        .map_err(|source| Error::transport(&Method::GET, "client builder", source))
}
