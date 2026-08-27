use crate::models::{Dataset, PushFileOptions, UploadModeOverride};
use crate::retry::{self, STORAGE_RETRY};
use crate::{Error, MarpleDB, ProgressReporter, Result};
use base64::Engine;
use futures_util::StreamExt;
use reqwest::{Body, Method, Response, header::CONTENT_LENGTH};
use serde::Deserialize;
use serde_json::Value;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;

const PROGRESS_STREAM_CHUNK: usize = 256 * 1024;

impl MarpleDB {
    /// Uploads a file to a stream and returns the created dataset.
    ///
    /// Pass [`PushFileOptions::default`] for local-file defaults, or chain
    /// setters for metadata, dataset name, overwrite, concurrency, upload
    /// mode, and progress reporting.
    #[tracing::instrument(skip_all, fields(stream_id, path = %file_path.as_ref().display()))]
    pub async fn push_file(
        &self,
        stream_id: i64,
        file_path: impl AsRef<Path>,
        options: PushFileOptions,
    ) -> Result<Dataset> {
        let file_path = file_path.as_ref();
        let file_name = file_name_from_path(file_path)?;
        let total_size = tokio::fs::metadata(file_path).await?.len();

        let init = self
            .init_ingestion(
                stream_id,
                options.dataset_name.as_deref().unwrap_or(&file_name),
                total_size,
                &options.metadata,
                options.overwrite,
            )
            .await?;
        let progress = Arc::clone(&options.progress);

        let upload_result = async {
            match effective_upload_mode(options.upload_mode, init.mode) {
                UploadMode::Server => {
                    tracing::debug!(ingestion_id = init.ingestion_id, "uploading via server");
                    self.upload_via_server(
                        &init,
                        file_path,
                        &file_name,
                        total_size,
                        Arc::clone(&progress),
                    )
                    .await?;
                }
                UploadMode::Azure => {
                    tracing::debug!(
                        ingestion_id = init.ingestion_id,
                        "uploading via Azure blocks"
                    );
                    self.upload_via_azure(
                        &init,
                        file_path,
                        total_size,
                        options.concurrency,
                        Arc::clone(&progress),
                    )
                    .await?;
                }
                UploadMode::Single => {
                    tracing::debug!(ingestion_id = init.ingestion_id, "uploading via single PUT");
                    self.upload_via_single(&init, file_path, total_size, Arc::clone(&progress))
                        .await?;
                }
                UploadMode::Multipart => {
                    tracing::debug!(ingestion_id = init.ingestion_id, "uploading via multipart");
                    self.upload_via_multipart(
                        &init,
                        file_path,
                        total_size,
                        options.concurrency,
                        Arc::clone(&progress),
                    )
                    .await?;
                }
            }
            self.complete_upload(init.ingestion_id).await?;
            self.get_dataset(stream_id, init.dataset_id).await
        }
        .await;

        match upload_result {
            Ok(dataset) => Ok(dataset),
            Err(e) => {
                let _ = self
                    .abort_upload(init.ingestion_id, &format!("{:#}", e))
                    .await;
                Err(e)
            }
        }
    }

    async fn upload_via_server(
        &self,
        init: &IngestionInit,
        file_path: &Path,
        file_name: &str,
        total_size: u64,
        progress: Arc<dyn ProgressReporter>,
    ) -> Result<()> {
        let file = tokio::fs::File::open(file_path).await?;
        let mut uploaded = 0;

        let mut reader = ReaderStream::new(file);
        let stream = async_stream::stream! {
            while let Some(chunk) = reader.next().await {
                if let Ok(chunk) = &chunk {
                    uploaded += chunk.len() as u64;
                    progress.set_position(uploaded);
                }
                yield chunk;
            }
            progress.finish();
        };

        let body = Body::wrap_stream(stream);
        let part = reqwest::multipart::Part::stream_with_length(body, total_size)
            .file_name(file_name.to_string())
            .mime_str("application/octet-stream")
            .map_err(|source| {
                Error::storage(
                    "building multipart upload body",
                    None,
                    None,
                    Some(source),
                )
            })?;
        let form = reqwest::multipart::Form::new().part("file", part);
        let endpoint = format!("ingestion/{}/upload/server", init.ingestion_id);
        self.post_multipart(&endpoint, form).await?;
        Ok(())
    }

    async fn upload_via_azure(
        &self,
        init: &IngestionInit,
        file_path: &Path,
        total_size: u64,
        concurrency: usize,
        progress: Arc<dyn ProgressReporter>,
    ) -> Result<()> {
        const AZURE_BLOCK_SIZE: u64 = 64 * 1024 * 1024;

        let url = init.presigned_url.as_deref().ok_or_else(|| {
            Error::Protocol("azure upload mode without presigned_url".to_string())
        })?;
        let sas_url: reqwest::Url = url.parse()?;

        let concurrency = concurrency.max(1);
        let descriptors = azure_block_descriptors(total_size, AZURE_BLOCK_SIZE);
        let context = AzureBlockUploadContext {
            sas_url: Arc::new(sas_url.clone()),
            file_path: file_path.to_path_buf(),
            uploaded: Arc::new(AtomicU64::new(0)),
            progress: Arc::clone(&progress),
        };
        let cursor = Arc::new(Mutex::new(descriptors.clone().into_iter()));

        let workers = (0..concurrency).map(|_| {
            let context = context.clone();
            let cursor = Arc::clone(&cursor);
            async move {
                let mut file = tokio::fs::File::open(&context.file_path).await?;
                loop {
                    let descriptor = {
                        let mut cursor = cursor.lock().await;
                        cursor.next()
                    };
                    let Some(descriptor) = descriptor else {
                        return Ok::<_, Error>(());
                    };

                    self.put_block(&mut file, &context, descriptor).await?;
                }
            }
        });
        futures_util::future::try_join_all(workers).await?;

        self.commit_azure_block_list(&sas_url, &descriptors).await?;
        progress.finish();
        Ok(())
    }

    async fn upload_via_single(
        &self,
        init: &IngestionInit,
        file_path: &Path,
        total_size: u64,
        progress: Arc<dyn ProgressReporter>,
    ) -> Result<()> {
        let url = init.presigned_url.as_deref().ok_or_else(|| {
            Error::Protocol("single upload mode without presigned_url".to_string())
        })?;
        let file = tokio::fs::File::open(file_path).await?;
        let mut uploaded = 0;

        let mut reader = ReaderStream::new(file);
        let stream = async_stream::stream! {
            while let Some(chunk) = reader.next().await {
                if let Ok(chunk) = &chunk {
                    uploaded += chunk.len() as u64;
                    progress.set_position(uploaded);
                }
                yield chunk;
            }
            progress.finish();
        };

        let response = send_storage(
            self.storage_client
                .put(url)
                .header(CONTENT_LENGTH, total_size)
                .body(Body::wrap_stream(stream)),
            "storage PUT failed",
        )
        .await?;
        ensure_success(response, "storage PUT failed").await?;
        Ok(())
    }

    async fn upload_via_multipart(
        &self,
        init: &IngestionInit,
        file_path: &Path,
        total_size: u64,
        concurrency: usize,
        progress: Arc<dyn ProgressReporter>,
    ) -> Result<()> {
        let part_size = init.part_size.ok_or_else(|| {
            Error::Protocol("multipart upload mode without part_size".to_string())
        })?;
        if part_size == 0 {
            return Err(Error::Protocol(
                "multipart upload part_size must be positive".to_string(),
            ));
        }
        let concurrency = concurrency.max(1);

        let uploaded = Arc::new(AtomicU64::new(0));
        let batch_size = concurrency.max(32);
        let context = MultipartUploadContext {
            file_path: file_path.to_path_buf(),
            part_size,
            total_size,
            uploaded,
            progress: Arc::clone(&progress),
        };
        let parts = self.signed_parts_stream(init.ingestion_id, batch_size);
        let parts = Arc::new(Mutex::new(Box::pin(parts)));

        let workers = (0..concurrency).map(|_| {
            let context = context.clone();
            let parts = Arc::clone(&parts);
            async move {
                let mut file = tokio::fs::File::open(&context.file_path).await?;
                loop {
                    let part = {
                        let mut parts = parts.lock().await;
                        parts.next().await.transpose()?
                    };
                    let Some(part) = part else {
                        return Ok::<_, Error>(());
                    };

                    self.put_part(&mut file, &context, part).await?;
                }
            }
        });
        futures_util::future::try_join_all(workers).await?;

        progress.finish();
        Ok(())
    }

    async fn init_ingestion(
        &self,
        stream_id: i64,
        dataset_name: &str,
        file_size: u64,
        metadata: &crate::Metadata,
        overwrite: bool,
    ) -> Result<IngestionInit> {
        let body = serde_json::json!({
            "stream_id": stream_id,
            "dataset_name": dataset_name,
            "file_size": file_size,
            "metadata": metadata,
            "overwrite": overwrite,
        });
        self.post("ingestion", &body).await
    }

    async fn complete_upload(&self, ingestion_id: i64) -> Result<()> {
        let endpoint = format!("ingestion/{}/upload/complete", ingestion_id);
        self.post::<_, Value>(&endpoint, &serde_json::json!({}))
            .await?;
        Ok(())
    }

    async fn abort_upload(&self, ingestion_id: i64, reason: &str) -> Result<()> {
        let endpoint = format!("ingestion/{}/abort", ingestion_id);
        self.post::<_, Value>(&endpoint, &serde_json::json!({ "reason": reason }))
            .await?;
        Ok(())
    }

    async fn put_block(
        &self,
        file: &mut tokio::fs::File,
        context: &AzureBlockUploadContext,
        descriptor: BlockDescriptor,
    ) -> Result<()> {
        file.seek(SeekFrom::Start(descriptor.offset)).await?;
        let mut data = vec![0; usize::try_from(descriptor.length)?];
        file.read_exact(&mut data).await?;

        let stream = progress_reporting_stream(
            data,
            Arc::clone(&context.uploaded),
            Arc::clone(&context.progress),
        );

        let mut block_url = (*context.sas_url).clone();
        block_url
            .query_pairs_mut()
            .append_pair("comp", "block")
            .append_pair(
                "blockid",
                &base64::engine::general_purpose::STANDARD.encode(descriptor.block_id.as_bytes()),
            );

        let response = send_storage(
            self.storage_client
                .put(block_url)
                .header(CONTENT_LENGTH, descriptor.length)
                .body(Body::wrap_stream(stream)),
            format!("Azure block {} upload failed", descriptor.block_id),
        )
        .await?;
        ensure_success(
            response,
            format!("Azure block {} upload failed", descriptor.block_id),
        )
        .await?;
        Ok(())
    }

    async fn commit_azure_block_list(
        &self,
        sas_url: &reqwest::Url,
        descriptors: &[BlockDescriptor],
    ) -> Result<()> {
        let mut block_list_url = sas_url.clone();
        block_list_url
            .query_pairs_mut()
            .append_pair("comp", "blocklist");

        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<BlockList>\n");
        for descriptor in descriptors {
            let block_id =
                base64::engine::general_purpose::STANDARD.encode(descriptor.block_id.as_bytes());
            xml.push_str("\t<Uncommitted>");
            xml.push_str(&block_id);
            xml.push_str("</Uncommitted>\n");
        }
        xml.push_str("</BlockList>");
        let date = httpdate::fmt_http_date(std::time::SystemTime::now());

        let response = send_storage(
            self.storage_client
                .put(block_list_url)
                .header(reqwest::header::CONTENT_TYPE, "application/xml")
                .header(reqwest::header::CONTENT_LENGTH, xml.len())
                .header("x-ms-date", date)
                .header("x-ms-version", "2022-11-02")
                .body(xml),
            "Azure block list commit failed",
        )
        .await?;
        ensure_success(response, "Azure block list commit failed").await
    }

    async fn put_part(
        &self,
        file: &mut tokio::fs::File,
        context: &MultipartUploadContext,
        part: PartUrl,
    ) -> Result<()> {
        let (offset, part_len) =
            part_byte_range(part.part_number, context.part_size, context.total_size)?;

        file.seek(SeekFrom::Start(offset)).await?;
        let mut data = vec![0; usize::try_from(part_len)?];
        file.read_exact(&mut data).await?;

        let stream = progress_reporting_stream(
            data,
            Arc::clone(&context.uploaded),
            Arc::clone(&context.progress),
        );

        let response = send_storage(
            self.storage_client
                .put(part.url)
                .header(CONTENT_LENGTH, part_len)
                .body(Body::wrap_stream(stream)),
            format!("part {} storage PUT failed", part.part_number),
        )
        .await?;
        ensure_success(
            response,
            format!("part {} storage PUT failed", part.part_number),
        )
        .await?;
        Ok(())
    }

    fn signed_parts_stream(
        &self,
        ingestion_id: i64,
        batch_size: usize,
    ) -> impl futures_util::Stream<Item = Result<PartUrl>> + '_ {
        async_stream::try_stream! {
            let mut next_part = Some(1);

            while let Some(start_part) = next_part {
                let urls = self.get_part_urls(ingestion_id, start_part, batch_size).await?;
                if urls.parts.is_empty() {
                    Err(Error::Protocol("server returned no multipart upload URLs".to_string()))?;
                }

                for part in urls.parts {
                    yield part;
                }

                next_part = urls.next_part;
            }
        }
    }

    async fn get_part_urls(
        &self,
        ingestion_id: i64,
        start_part: u32,
        count: usize,
    ) -> Result<PartUrlsResponse> {
        let endpoint = format!("ingestion/{}/upload/part-urls", ingestion_id);
        self.get(
            &endpoint,
            &[("start_part", start_part), ("count", count as u32)],
        )
        .await
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum UploadMode {
    Server,
    Azure,
    Single,
    Multipart,
}

#[derive(Debug, Deserialize)]
struct IngestionInit {
    dataset_id: i64,
    ingestion_id: i64,
    mode: UploadMode,
    presigned_url: Option<String>,
    part_size: Option<u64>,
    #[serde(rename = "expires_in")]
    _expires_in: u64,
}

#[derive(Debug, Deserialize)]
struct PartUrl {
    part_number: u32,
    url: String,
}

#[derive(Debug, Deserialize)]
struct PartUrlsResponse {
    parts: Vec<PartUrl>,
    #[serde(rename = "expires_in")]
    _expires_in: u64,
    next_part: Option<u32>,
}

#[derive(Clone)]
struct MultipartUploadContext {
    file_path: PathBuf,
    part_size: u64,
    total_size: u64,
    uploaded: Arc<AtomicU64>,
    progress: Arc<dyn ProgressReporter>,
}

#[derive(Clone)]
struct AzureBlockUploadContext {
    sas_url: Arc<reqwest::Url>,
    file_path: PathBuf,
    uploaded: Arc<AtomicU64>,
    progress: Arc<dyn ProgressReporter>,
}

#[derive(Clone)]
struct BlockDescriptor {
    offset: u64,
    length: u64,
    block_id: String,
}

fn file_name_from_path(file_path: &Path) -> Result<String> {
    file_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| {
            Error::Config(format!(
                "upload path {} has no file name",
                file_path.display()
            ))
        })
}

fn effective_upload_mode(override_mode: UploadModeOverride, mode: UploadMode) -> UploadMode {
    match override_mode {
        UploadModeOverride::Server => UploadMode::Server,
        UploadModeOverride::Auto => mode,
    }
}

fn part_byte_range(part_number: u32, part_size: u64, total_size: u64) -> Result<(u64, u64)> {
    if part_number == 0 {
        return Err(Error::Protocol(
            "multipart part numbers are 1-based".to_string(),
        ));
    }
    let offset = u64::from(part_number - 1) * part_size;
    if offset >= total_size {
        return Err(Error::Protocol(format!(
            "part {part_number} offset is outside the file"
        )));
    }
    Ok((offset, part_size.min(total_size - offset)))
}

fn azure_block_descriptors(total_size: u64, block_size: u64) -> Vec<BlockDescriptor> {
    if total_size == 0 {
        return Vec::new();
    }

    let n_blocks = total_size.div_ceil(block_size);
    (0..n_blocks as u32)
        .map(|block_number| {
            let offset = u64::from(block_number) * block_size;
            let length = block_size.min(total_size - offset);
            BlockDescriptor {
                offset,
                length,
                block_id: format!("{block_number:08}"),
            }
        })
        .collect()
}

fn progress_reporting_stream(
    data: Vec<u8>,
    uploaded: Arc<AtomicU64>,
    progress: Arc<dyn ProgressReporter>,
) -> impl futures_util::Stream<Item = std::io::Result<Vec<u8>>> + Send + 'static {
    async_stream::stream! {
        let mut pos = 0;
        while pos < data.len() {
            let end = (pos + PROGRESS_STREAM_CHUNK).min(data.len());
            let chunk = data[pos..end].to_vec();
            let chunk_len = chunk.len() as u64;
            let new_uploaded = uploaded.fetch_add(chunk_len, Ordering::Relaxed) + chunk_len;
            progress.set_position(new_uploaded);
            yield Ok(chunk);
            pos = end;
        }
    }
}

async fn send_storage(
    request: reqwest::RequestBuilder,
    context: impl Into<String>,
) -> Result<Response> {
    let context = context.into();
    retry::send_with_retry(request, &Method::PUT, &STORAGE_RETRY)
        .await
        .map_err(|source| Error::storage(context, None, None, Some(source)))
}

async fn ensure_success(response: Response, failure_message: impl Into<String>) -> Result<()> {
    if response.status().is_success() {
        Ok(())
    } else {
        let context = failure_message.into();
        let status = response.status();
        let body = response.text().await.map_err(|source| {
            Error::storage(context.clone(), Some(status), None, Some(source))
        })?;
        Err(Error::storage(context, Some(status), Some(body), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn file_name_from_path_rejects_roots_and_empty() {
        assert!(file_name_from_path(Path::new("/")).is_err());
        assert!(file_name_from_path(Path::new("")).is_err());
        assert_eq!(
            file_name_from_path(Path::new("run.csv")).unwrap(),
            "run.csv"
        );
        assert_eq!(
            file_name_from_path(Path::new("/tmp/heat1.mf4")).unwrap(),
            "heat1.mf4"
        );
    }

    #[test]
    fn server_override_wins_over_storage_modes() {
        for mode in [
            UploadMode::Server,
            UploadMode::Azure,
            UploadMode::Single,
            UploadMode::Multipart,
        ] {
            assert_eq!(
                effective_upload_mode(UploadModeOverride::Server, mode),
                UploadMode::Server
            );
            assert_eq!(effective_upload_mode(UploadModeOverride::Auto, mode), mode);
        }
    }

    #[test]
    fn azure_blocks_cover_the_file_without_overlap() {
        assert!(azure_block_descriptors(0, 64).is_empty());

        let descriptors = azure_block_descriptors(150, 64);
        assert_eq!(descriptors.len(), 3);
        assert_eq!(descriptors[0].offset, 0);
        assert_eq!(descriptors[0].length, 64);
        assert_eq!(descriptors[0].block_id, "00000000");
        assert_eq!(descriptors[1].offset, 64);
        assert_eq!(descriptors[1].length, 64);
        assert_eq!(descriptors[2].offset, 128);
        assert_eq!(descriptors[2].length, 22);
        assert_eq!(descriptors.iter().map(|d| d.length).sum::<u64>(), 150);
    }

    #[test]
    fn multipart_part_ranges_are_1_based() {
        assert_eq!(part_byte_range(1, 10, 25).unwrap(), (0, 10));
        assert_eq!(part_byte_range(2, 10, 25).unwrap(), (10, 10));
        assert_eq!(part_byte_range(3, 10, 25).unwrap(), (20, 5));
        assert!(part_byte_range(0, 10, 25).is_err());
        assert!(part_byte_range(4, 10, 25).is_err());
    }
}
