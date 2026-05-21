use crate::errors::{Error, Result};
use crate::models::{Dataset, HealthResponse, ImportStatus, StreamsResponse};
use reqwest::{
    Client, Method, Response, Url,
    header::{AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT},
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::time::Duration;

/// Client for the MarpleDB API.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct MarpleDB {
    pub(crate) client: Client,
    pub(crate) storage_client: Client,
    pub(crate) base_url: String,
    auth_header: HeaderValue,
}

impl MarpleDB {
    /// Creates a new client for `url` using a bearer API token.
    pub fn new(url: &str, token: &str) -> Result<Self> {
        Self::builder().url(url).token(token).build()
    }

    /// Creates a builder for configuring a client.
    pub fn builder() -> MarpleDBBuilder {
        MarpleDBBuilder::default()
    }

    /// Returns the header-free storage client used for pre-signed download and upload URLs.
    pub fn storage_client(&self) -> &Client {
        &self.storage_client
    }

    fn url(&self, endpoint: &str) -> String {
        self.base_url.clone() + endpoint.trim_start_matches('/')
    }

    fn auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header(AUTHORIZATION, self.auth_header.clone())
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
        let response = request.send().await.map_err(|source| Error::Transport {
            method: method.clone(),
            endpoint: endpoint.to_string(),
            source,
        })?;
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
        let body = response.text().await.map_err(|source| Error::Transport {
            method: method.clone(),
            endpoint: endpoint.to_string(),
            source,
        })?;
        if !status.is_success() {
            return Err(Error::Api {
                method,
                endpoint: endpoint.to_string(),
                status,
                body,
            });
        }
        Ok(serde_json::from_str(&body)?)
    }

    /// Sends a GET request and deserializes the JSON response.
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
    #[tracing::instrument(skip_all, fields(endpoint = %endpoint))]
    pub async fn post<B, R>(&self, endpoint: &str, body: &B) -> Result<R>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let request = self.auth(self.client.post(self.url(endpoint)).json(body));
        self.send_json(endpoint, Method::POST, request).await
    }

    /// Sends a DELETE request with a JSON body and deserializes the JSON response.
    #[tracing::instrument(skip_all, fields(endpoint = %endpoint))]
    pub async fn delete<B, R>(&self, endpoint: &str, body: &B) -> Result<R>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let request = self.auth(self.client.delete(self.url(endpoint)).json(body));
        self.send_json(endpoint, Method::DELETE, request).await
    }

    pub(crate) async fn post_json<B, R>(&self, endpoint: &str, body: &B) -> Result<R>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.post(endpoint, body).await
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

    pub(crate) async fn get_json<Q, R>(&self, endpoint: &str, query: &Q) -> Result<R>
    where
        Q: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.get(endpoint, query).await
    }

    /// Checks MarpleDB API health.
    pub async fn health(&self) -> Result<HealthResponse> {
        self.get("health", &()).await
    }

    /// Lists all streams visible to the token.
    pub async fn get_streams(&self) -> Result<Vec<crate::Stream>> {
        let streams_response: StreamsResponse = self.get("streams", &()).await?;
        Ok(streams_response.streams)
    }

    /// Finds a stream by name.
    pub async fn get_stream(&self, stream_name: &str) -> Result<crate::Stream> {
        let streams = self.get_streams().await?;
        streams
            .into_iter()
            .find(|s| s.name == stream_name)
            .ok_or_else(|| Error::StreamNotFound {
                name: stream_name.to_string(),
            })
    }

    /// Creates a stream with a name and serializable options object.
    pub async fn create_stream<S: Serialize + ?Sized>(
        &self,
        stream_name: &str,
        options: &S,
    ) -> Result<crate::Stream> {
        let mut options = match serde_json::to_value(options)? {
            Value::Object(options) => options,
            _ => {
                return Err(Error::Protocol(
                    "create_stream options must serialize to a JSON object".to_string(),
                ));
            }
        };
        options.insert("name".to_string(), Value::String(stream_name.to_string()));
        self.post_json::<_, Value>("stream", &options).await?;
        self.get_stream(stream_name).await
    }

    /// Updates a stream with a serializable options object.
    pub async fn update_stream<S: Serialize + ?Sized>(
        &self,
        stream_id: i32,
        options: &S,
    ) -> Result<crate::Stream> {
        let endpoint = format!("stream/update/{}", stream_id);
        self.post_json::<_, Value>(&endpoint, options).await?;
        self.get_streams()
            .await?
            .into_iter()
            .find(|stream| stream.id == stream_id)
            .ok_or(Error::StreamIdNotFound { id: stream_id })
    }

    /// Lists datasets in a stream.
    pub async fn get_datasets(&self, stream_id: i32) -> Result<Vec<Dataset>> {
        self.get(&format!("stream/{}/datasets", stream_id), &())
            .await
    }

    /// Fetches a dataset by stream id and dataset id.
    pub async fn get_dataset(&self, stream_id: i32, dataset_id: i32) -> Result<Dataset> {
        self.get(&format!("stream/{}/dataset/{}", stream_id, dataset_id), &())
            .await
    }

    /// Returns a pre-signed URL for downloading a dataset's original uploaded file.
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
        let link: DownloadLink = self.get_json(&endpoint, &()).await?;
        Ok(link.path.parse()?)
    }

    /// Waits until an import reaches a terminal status or times out.
    pub async fn wait_for_import(
        &self,
        stream_id: i32,
        dataset_id: i32,
        timeout: Duration,
    ) -> Result<Dataset> {
        let deadline = std::time::Instant::now() + timeout;
        let mut last_status = "unknown".to_string();

        while std::time::Instant::now() < deadline {
            let dataset = self.get_dataset(stream_id, dataset_id).await?;
            last_status = format!("{:?}", dataset.import_status);

            match dataset.import_status {
                ImportStatus::Finished | ImportStatus::Live => return Ok(dataset),
                ImportStatus::Failed | ImportStatus::PostprocessingFailed => {
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
}

/// Builder for `MarpleDB`.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct MarpleDBBuilder {
    url: Option<String>,
    token: Option<String>,
    client: Option<Client>,
    storage_client: Option<Client>,
    timeout: Option<Duration>,
    user_agent: Option<String>,
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
        }
    }
}

impl MarpleDBBuilder {
    /// Sets the MarpleDB API base URL.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Sets the bearer API token.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Sets the timeout for the API and storage HTTP clients built by the SDK.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the user agent for HTTP clients built by the SDK.
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Uses a caller-provided API HTTP client.
    pub fn client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Uses a caller-provided storage HTTP client.
    pub fn storage_client(mut self, client: Client) -> Self {
        self.storage_client = Some(client);
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
        let mut auth_header = HeaderValue::from_str(&format!("Bearer {}", token))?;
        auth_header.set_sensitive(true);

        let client = match self.client {
            Some(client) => client,
            None => build_client(self.timeout, self.user_agent.as_deref())?,
        };
        let storage_client = match self.storage_client {
            Some(client) => client,
            None => build_client(self.timeout, self.user_agent.as_deref())?,
        };

        Ok(MarpleDB {
            client,
            storage_client,
            base_url: url.trim_end_matches('/').to_string() + "/",
            auth_header,
        })
    }
}

fn build_client(timeout: Option<Duration>, user_agent: Option<&str>) -> Result<Client> {
    let mut builder = Client::builder();
    if let Some(timeout) = timeout {
        builder = builder.timeout(timeout);
    }
    if let Some(user_agent) = user_agent {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_str(user_agent)?);
        builder = builder.default_headers(headers);
    }
    builder.build().map_err(|source| Error::Transport {
        method: Method::GET,
        endpoint: "client builder".to_string(),
        source,
    })
}
