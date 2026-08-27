use crate::errors::{Error, Result};
use crate::models::{
    CurrentWorkspace, Dataset, HealthResponse, Settings, Signal, Stream, UsageSeries, UsageType,
    UserInfo, WorkspaceLicense,
};
use crate::progress::{NoopProgress, ProgressReporter};
use crate::retry::{self, API_RETRY, STORAGE_RETRY};
use futures_util::StreamExt;
use reqwest::{
    Client, Method,
    header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue, USER_AGENT},
};
use serde::{Serialize, de::DeserializeOwned};
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
    /// Use the builder for custom timeouts, a user agent, or caller-provided
    /// `reqwest::Client` instances.
    pub fn builder() -> MarpleDBBuilder {
        MarpleDBBuilder::default()
    }

    /// Checks MarpleDB API health.
    pub async fn health(&self) -> Result<HealthResponse> {
        self.get_json("health").await
    }

    /// Fetches workspace settings (`/settings`).
    ///
    /// The payload is a mix of Insight, storage-path, and build keys. Unknown
    /// keys are kept on [`Settings::extra`].
    pub async fn get_settings(&self) -> Result<Settings> {
        self.get_json("settings").await
    }

    /// Lists metadata field names used by datasets in a stream.
    pub async fn get_metadata_fields(&self, stream_id: i64) -> Result<Vec<String>> {
        self.get_json(&format!("stream/{stream_id}/metadata/fields"))
            .await
    }

    /// Fetches the license for the workspace bound to this token.
    ///
    /// Returns `Ok(None)` when the API returns JSON `null` or HTTP 404. The
    /// top-level `workspace` field is the workspace slug, not the license row id.
    pub async fn get_workspace_license(&self) -> Result<Option<WorkspaceLicense>> {
        Self::map_404(self.get_json("workspace/license").await, || Ok(None))
    }

    /// Fetches the authenticated user profile, including workspace memberships.
    pub async fn get_user_info(&self) -> Result<UserInfo> {
        self.get_json("user/info").await
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
            .ok_or_else(|| Error::protocol("could not resolve current workspace from /user/info"))?
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
        Ok(self.get_json::<StreamsResponse>("streams").await?.streams)
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
        Self::map_404(self.get_json(&format!("stream/{stream_id}")).await, || {
            Err(Error::StreamIdNotFound { id: stream_id })
        })
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
                return Err(Error::protocol(
                    "create_stream options must serialize to a JSON object",
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
        self.get_json(&format!("stream/{stream_id}/datasets")).await
    }

    /// Lists all datasets in a datapool.
    pub async fn get_datapool_datasets(&self, pool: &str) -> Result<Vec<Dataset>> {
        self.get_json(&format!("datapool/{pool}/datasets")).await
    }

    /// Lists datasets currently in the ingest queue for a datapool.
    pub async fn get_datapool_ingest_queue(&self, pool: &str) -> Result<Vec<Dataset>> {
        self.get_json(&format!("datapool/{pool}/ingest/queue"))
            .await
    }

    /// Fetches a dataset by stream id and dataset id.
    pub async fn get_dataset(&self, stream_id: i64, dataset_id: i64) -> Result<Dataset> {
        self.get_json(&format!("stream/{stream_id}/dataset/{dataset_id}"))
            .await
    }

    /// Lists signals in a dataset.
    pub async fn get_signals(&self, stream_id: i64, dataset_id: i64) -> Result<Vec<Signal>> {
        let mut signals: Vec<Signal> = self
            .get_json(&format!("stream/{stream_id}/dataset/{dataset_id}/signals"))
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
            "stream/{stream_id}/dataset/{dataset_id}/backup",
            stream_id = dataset.datastream_id,
            dataset_id = dataset.id
        );
        #[derive(serde::Deserialize)]
        struct DownloadLink {
            path: String,
        }
        let link: DownloadLink = self.get_json(&endpoint).await?;
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

        let response = send_storage(
            self.storage_client.get(url),
            &Method::GET,
            "storage GET failed",
        )
        .await?;
        let response = ensure_storage_success(response, "storage GET failed").await?;

        let mut file = tokio::fs::File::create(&path).await?;
        let mut downloaded = 0u64;
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk
                .map_err(|source| Error::storage("storage GET failed", None, None, Some(source)))?;
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
        let mut last_status = "unknown".to_string();
        let poll = async {
            loop {
                let dataset = self.get_dataset(stream_id, dataset_id).await?;
                last_status = dataset.import_status.to_string();

                if dataset.import_status.is_failure() {
                    return Err(Error::ImportFailed {
                        id: dataset.id,
                        message: dataset
                            .import_message
                            .unwrap_or_else(|| dataset.import_status.to_string()),
                    });
                }

                if dataset.import_status.is_success() {
                    return Ok(dataset);
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        };

        match tokio::time::timeout(timeout, poll).await {
            Ok(result) => result,
            Err(_) => Err(Error::ImportTimeout {
                timeout_secs: timeout.as_secs(),
                last_status,
            }),
        }
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
        self.send_json(Method::GET, endpoint, |request| request.query(query))
            .await
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
        self.send_json(Method::POST, endpoint, |request| request.json(body))
            .await
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
        self.send_json(Method::PATCH, endpoint, |request| request.json(body))
            .await
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
        self.send_json(Method::DELETE, endpoint, |request| request.json(body))
            .await
    }

    fn url(&self, endpoint: &str) -> String {
        self.base_url.clone() + endpoint.trim_start_matches('/')
    }

    async fn get_json<R: DeserializeOwned>(&self, endpoint: &str) -> Result<R> {
        self.get(endpoint, &()).await
    }

    fn map_404<T>(result: Result<T>, on_404: impl FnOnce() -> Result<T>) -> Result<T> {
        match result {
            Err(Error::Api { status: 404, .. }) => on_404(),
            result => result,
        }
    }

    async fn send_json<R>(
        &self,
        method: Method,
        endpoint: &str,
        build: impl FnOnce(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    ) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let request = build(self.client.request(method.clone(), self.url(endpoint)))
            .header(AUTHORIZATION, self.auth_header.clone())
            .header(REQUEST_SOURCE_HEADER, self.request_source.clone());
        let response = retry::send_with_retry(request, &method, &API_RETRY)
            .await
            .map_err(|source| Error::transport(&method, endpoint, source))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| Error::transport(&method, endpoint, source))?;
        if status.is_success() {
            Ok(serde_json::from_str(&body)?)
        } else {
            Err(Error::api(method, endpoint, status, body))
        }
    }

    #[tracing::instrument(skip_all, fields(endpoint = %endpoint))]
    pub(crate) async fn post_multipart(
        &self,
        endpoint: &str,
        form: reqwest::multipart::Form,
    ) -> Result<Value> {
        self.send_json(Method::POST, endpoint, |request| request.multipart(form))
            .await
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
    client: Option<Client>,
    storage_client: Option<Client>,
    timeout: Option<Duration>,
    user_agent: Option<String>,
    request_source: Option<String>,
}

impl Default for MarpleDBBuilder {
    fn default() -> Self {
        Self {
            url: None,
            token: None,
            client: None,
            storage_client: None,
            timeout: None,
            user_agent: Some(format!("marple-db/{}", env!("CARGO_PKG_VERSION"))),
            request_source: None,
        }
    }
}

impl MarpleDBBuilder {
    /// Sets the MarpleDB API base URL.
    ///
    /// The URL should usually end in `/api/v1`. [`crate::SAAS_URL`] is the
    /// hosted default.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Sets the bearer API token.
    ///
    /// The token is sent as `Authorization: Bearer <token>` on API requests.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Sets the total timeout for the API and storage HTTP clients built by the SDK.
    ///
    /// Defaults match the Python SDK: 300s for API requests and 1800s for
    /// storage. This override applies the same total timeout to both SDK-built
    /// clients. Caller-provided clients keep their own timeout configuration.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the user agent for HTTP clients built by the SDK.
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
    pub fn request_source(mut self, request_source: impl Into<String>) -> Self {
        self.request_source = Some(request_source.into());
        self
    }

    /// Uses a caller-provided API HTTP client.
    ///
    /// The SDK still attaches the MarpleDB authorization header per request.
    /// This takes `reqwest::Client` and follows reqwest's semver; prefer
    /// [`MarpleDB::new`] unless you need custom TLS or other client settings.
    pub fn client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Uses a caller-provided storage HTTP client.
    ///
    /// This client is used for pre-signed direct storage URLs and should not
    /// include MarpleDB authorization headers by default. Follows reqwest's
    /// semver; see [`Self::client`].
    pub fn storage_client(mut self, client: Client) -> Self {
        self.storage_client = Some(client);
        self
    }

    /// Builds a configured `MarpleDB` client.
    pub fn build(self) -> Result<MarpleDB> {
        let url = self
            .url
            .ok_or_else(|| Error::config("missing MarpleDB API URL"))?;
        let token = self
            .token
            .ok_or_else(|| Error::config("missing MarpleDB API token"))?;
        let mut auth_header = header_value(&format!("Bearer {token}"))?;
        auth_header.set_sensitive(true);

        let request_source = match self.request_source {
            Some(value) => header_value(&value)?,
            None => DEFAULT_REQUEST_SOURCE,
        };

        let client = match self.client {
            Some(client) => client,
            None => {
                let timeout = self.timeout.unwrap_or(MarpleDB::API_TIMEOUT);
                build_client(
                    timeout,
                    MarpleDB::API_CONNECT_TIMEOUT.min(timeout),
                    self.user_agent.as_deref(),
                )?
            }
        };
        let storage_client = match self.storage_client {
            Some(client) => client,
            None => {
                let timeout = self.timeout.unwrap_or(MarpleDB::STORAGE_TIMEOUT);
                let connect = if self.timeout.is_some() {
                    MarpleDB::API_CONNECT_TIMEOUT.min(timeout)
                } else {
                    MarpleDB::STORAGE_TIMEOUT
                };
                build_client(timeout, connect, self.user_agent.as_deref())?
            }
        };

        Ok(MarpleDB {
            client,
            storage_client,
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
        .map_err(|error| Error::config(format!("invalid HTTP header value: {error}")))
}

fn build_client(
    timeout: Duration,
    connect_timeout: Duration,
    user_agent: Option<&str>,
) -> Result<Client> {
    let mut builder = Client::builder()
        .timeout(timeout)
        .connect_timeout(connect_timeout);
    if let Some(user_agent) = user_agent {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, header_value(user_agent)?);
        builder = builder.default_headers(headers);
    }
    builder
        .build()
        .map_err(|source| Error::transport(&Method::GET, "client builder", source))
}

pub(crate) async fn send_storage(
    request: reqwest::RequestBuilder,
    method: &Method,
    context: impl Into<String>,
) -> Result<reqwest::Response> {
    let context = context.into();
    retry::send_with_retry(request, method, &STORAGE_RETRY)
        .await
        .map_err(|source| Error::storage(context, None, None, Some(source)))
}

pub(crate) async fn ensure_storage_success(
    response: reqwest::Response,
    context: impl Into<String>,
) -> Result<reqwest::Response> {
    if response.status().is_success() {
        Ok(response)
    } else {
        let context = context.into();
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| Error::storage(context.clone(), Some(status), None, Some(source)))?;
        Err(Error::storage(context, Some(status), Some(body), None))
    }
}

pub(crate) async fn put_storage(
    request: reqwest::RequestBuilder,
    context: impl Into<String>,
) -> Result<()> {
    let context = context.into();
    let response = send_storage(request, &Method::PUT, context.clone()).await?;
    ensure_storage_success(response, context).await?;
    Ok(())
}
