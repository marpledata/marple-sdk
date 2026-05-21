use crate::models::{BackupResponse, Dataset, HealthResponse, Metadata, StreamsResponse};
use anyhow::{Result, anyhow};
use futures_util::StreamExt;
use reqwest::{
    Client, Response,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::path::Path;
use tokio::io::AsyncWriteExt;

pub struct MarpleDB {
    pub(crate) client: Client,
    pub(crate) storage_client: Client,
    pub(crate) base_url: String,
}

impl MarpleDB {
    pub fn new(url: &str, token: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        let mut bearer = HeaderValue::from_str(&format!("Bearer {}", token))?;
        bearer.set_sensitive(true);
        headers.insert(AUTHORIZATION, bearer);
        let client = Client::builder().default_headers(headers).build()?;
        // Direct storage URLs are pre-signed/SAS-authenticated, so do not send Marple DB headers.
        let storage_client = Client::new();

        Ok(Self {
            client,
            storage_client,
            base_url: url.trim_end_matches('/').to_string() + "/",
        })
    }

    async fn handle_response(
        &self,
        endpoint: &str,
        method: &str,
        response: Response,
    ) -> Result<Value> {
        if response.status().is_success() {
            Ok(response.json().await?)
        } else {
            Err(anyhow!(
                "{} {} failed with status {}: {}",
                method,
                endpoint,
                response.status(),
                response.text().await?
            ))
        }
    }

    pub async fn get(&self, endpoint: &str, params: Option<Vec<(String, Value)>>) -> Result<Value> {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let mut request = self.client.get(&url);

        if let Some(params) = params {
            request = request.query(&to_record(params));
        }
        let response = request.send().await?;
        self.handle_response(endpoint, "GET", response).await
    }

    pub async fn post(&self, endpoint: &str, data: Option<Vec<(String, Value)>>) -> Result<Value> {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let mut request = self.client.post(&url);

        if let Some(data) = data {
            request = request.json(&to_record(data));
        }
        let response = request.send().await?;
        self.handle_response(endpoint, "POST", response).await
    }

    pub async fn delete(
        &self,
        endpoint: &str,
        json: Option<Vec<(String, Value)>>,
    ) -> Result<Value> {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let mut request = self.client.delete(&url);

        if let Some(json) = json {
            request = request.json(&to_record(json));
        }
        let response = request.send().await?;
        self.handle_response(endpoint, "DELETE", response).await
    }

    pub(crate) async fn post_json<B, R>(&self, endpoint: &str, body: &B) -> Result<R>
    where
        B: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let response = self.client.post(url).json(body).send().await?;
        let response = self.handle_response(endpoint, "POST", response).await?;
        Ok(serde_json::from_value(response)?)
    }

    pub(crate) async fn post_multipart(
        &self,
        endpoint: &str,
        form: reqwest::multipart::Form,
    ) -> Result<Value> {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let response = self.client.post(url).multipart(form).send().await?;
        self.handle_response(endpoint, "POST", response).await
    }

    pub(crate) async fn get_json<Q, R>(&self, endpoint: &str, query: &Q) -> Result<R>
    where
        Q: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let url = self.base_url.clone() + endpoint.trim_start_matches('/');
        let response = self.client.get(url).query(query).send().await?;
        let response = self.handle_response(endpoint, "GET", response).await?;
        Ok(serde_json::from_value(response)?)
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        let response = self.get("health", None).await?;
        Ok(serde_json::from_value(response)?)
    }

    pub async fn get_streams(&self) -> Result<Vec<crate::Stream>> {
        let response = self.get("streams", None).await?;
        let streams_response: StreamsResponse = serde_json::from_value(response)?;
        Ok(streams_response.streams)
    }

    pub async fn get_stream(&self, stream_name: &str) -> Result<crate::Stream> {
        let streams = self.get_streams().await?;
        streams
            .into_iter()
            .find(|s| s.name == stream_name)
            .ok_or_else(|| anyhow!("stream {} not found", stream_name))
    }

    pub async fn create_stream(
        &self,
        stream_name: &str,
        options: &Metadata,
    ) -> Result<crate::Stream> {
        let mut options = options.clone();
        options.insert("name".to_string(), Value::String(stream_name.to_string()));
        self.post_json::<_, Value>("stream", &options).await?;
        self.get_stream(stream_name).await
    }

    pub async fn update_stream(&self, stream_id: i32, options: &Metadata) -> Result<crate::Stream> {
        let endpoint = format!("stream/update/{}", stream_id);
        self.post_json::<_, Value>(&endpoint, options).await?;
        self.get_streams()
            .await?
            .into_iter()
            .find(|stream| stream.id == stream_id)
            .ok_or_else(|| anyhow!("stream {} not found", stream_id))
    }

    pub async fn get_datasets(&self, stream_id: i32) -> Result<Vec<Dataset>> {
        let response = self
            .get(&format!("stream/{}/datasets", stream_id), None)
            .await?;
        let datasets: Vec<Dataset> = response
            .as_array()
            .ok_or(anyhow!("Expected an array of datasets"))?
            .iter()
            .map(|d| serde_json::from_value(d.clone()))
            .collect::<std::result::Result<_, _>>()?;
        Ok(datasets)
    }

    pub async fn get_dataset(&self, stream_id: i32, dataset_id: i32) -> Result<Dataset> {
        let response = self
            .get(
                &format!("stream/{}/dataset/{}", stream_id, dataset_id),
                None,
            )
            .await?;
        Ok(serde_json::from_value(response)?)
    }

    pub async fn download_dataset(
        &self,
        dataset: &Dataset,
        output_dir: Option<&Path>,
        progress: &dyn crate::ProgressReporter,
    ) -> Result<String> {
        let Some(backup_size) = dataset.backup_size else {
            return Err(anyhow!("Dataset {} has no backup", dataset.id));
        };
        let endpoint = format!(
            "stream/{}/dataset/{}/backup",
            dataset.datastream_id, dataset.id
        );
        let BackupResponse { path } = self.get_json(&endpoint, &()).await?;
        let local_path = output_dir
            .unwrap_or_else(|| Path::new("."))
            .join(dataset.path.clone());
        let mut file = tokio::fs::File::create(local_path.clone()).await?;
        let mut bytes_stream = reqwest::get(path).await?.bytes_stream();
        let mut downloaded = 0;

        while let Some(chunk) = bytes_stream.next().await {
            if let Ok(chunk) = &chunk {
                file.write_all(chunk).await?;
                downloaded += chunk.len() as u64;
                progress.set_position(downloaded.min(backup_size));
            }
        }
        progress.finish();
        Ok(local_path.to_string_lossy().to_string())
    }
}

fn to_record(pairs: Vec<(String, Value)>) -> Metadata {
    pairs.into_iter().collect()
}
