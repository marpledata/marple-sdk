use crate::models::{
    Dataset, IngestionInit, PartUrl, PartUrlsResponse, PushFileOptions, UploadMode,
    UploadModeOverride,
};
use crate::{Error, MarpleDB, ProgressReporter, Result};
use base64::Engine;
use futures_util::StreamExt;
use reqwest::{Body, Response, header::CONTENT_LENGTH};
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

#[derive(Clone)]
struct MultipartUploadContext {
    file_path: PathBuf,
    part_size: u64,
    total_size: u64,
    uploaded: Arc<AtomicU64>,
    progress: Arc<dyn ProgressReporter>,
}

#[derive(Clone)]
struct BlockDescriptor {
    offset: u64,
    length: u64,
    block_id: String,
}

#[derive(Clone)]
struct AzureBlockUploadContext {
    sas_url: Arc<reqwest::Url>,
    file_path: PathBuf,
    uploaded: Arc<AtomicU64>,
    progress: Arc<dyn ProgressReporter>,
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

async fn ensure_success(response: Response, failure_message: impl Into<String>) -> Result<()> {
    if response.status().is_success() {
        Ok(())
    } else {
        let context = failure_message.into();
        let status = response.status();
        let body = response.text().await.map_err(|source| Error::Storage {
            context: context.clone(),
            status: Some(status),
            body: None,
            source: Some(source),
        })?;
        Err(Error::Storage {
            context,
            status: Some(status),
            body: Some(body),
            source: None,
        })
    }
}

async fn send_storage(
    request: reqwest::RequestBuilder,
    context: impl Into<String>,
) -> Result<Response> {
    let context = context.into();
    request.send().await.map_err(|source| Error::Storage {
        context,
        status: None,
        body: None,
        source: Some(source),
    })
}

impl MarpleDB {
    async fn init_ingestion(
        &self,
        stream_id: i32,
        dataset_name: &str,
        file_size: u64,
        metadata: &crate::Metadata,
    ) -> Result<IngestionInit> {
        let body = serde_json::json!({
            "stream_id": stream_id,
            "dataset_name": dataset_name,
            "file_size": file_size,
            "metadata": metadata,
        });
        self.post_json("ingestion", &body).await
    }

    async fn get_part_urls(
        &self,
        ingestion_id: i32,
        start_part: u32,
        count: usize,
    ) -> Result<PartUrlsResponse> {
        let endpoint = format!("ingestion/{}/upload/part-urls", ingestion_id);
        self.get_json(
            &endpoint,
            &[("start_part", start_part), ("count", count as u32)],
        )
        .await
    }

    async fn complete_upload(&self, ingestion_id: i32) -> Result<()> {
        let endpoint = format!("ingestion/{}/upload/complete", ingestion_id);
        self.post_json::<_, Value>(&endpoint, &serde_json::json!({}))
            .await?;
        Ok(())
    }

    async fn abort_upload(&self, ingestion_id: i32, reason: &str) -> Result<()> {
        let endpoint = format!("ingestion/{}/abort", ingestion_id);
        self.post_json::<_, Value>(&endpoint, &serde_json::json!({ "reason": reason }))
            .await?;
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
            .map_err(|source| Error::Storage {
                context: "building multipart upload body".to_string(),
                status: None,
                body: None,
                source: Some(source),
            })?;
        let form = reqwest::multipart::Form::new().part("file", part);
        let endpoint = format!("ingestion/{}/upload/server", init.ingestion_id);
        self.post_multipart(&endpoint, form).await?;
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
        let offset = u64::from(part.part_number - 1) * context.part_size;
        if offset >= context.total_size {
            return Err(Error::Protocol(format!(
                "part {} offset is outside the file",
                part.part_number
            )));
        }
        let part_len = context.part_size.min(context.total_size - offset);

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
        ingestion_id: i32,
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

    /// Uploads a file to a stream and returns the created dataset.
    #[tracing::instrument(skip_all, fields(stream_id, path = %file_path.as_ref().display()))]
    pub async fn push_file(
        &self,
        stream_id: i32,
        file_path: impl AsRef<Path>,
        options: PushFileOptions,
    ) -> Result<Dataset> {
        let file_path = file_path.as_ref();
        let file_name = file_path.file_name().unwrap().to_string_lossy().to_string();
        let total_size = tokio::fs::metadata(file_path).await?.len();

        let init = self
            .init_ingestion(stream_id, &file_name, total_size, &options.metadata)
            .await?;
        let progress = Arc::clone(&options.progress);

        let upload_result = async {
            match (options.upload_mode, &init.mode) {
                (UploadModeOverride::Server, _) | (_, UploadMode::Server) => {
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
                (_, UploadMode::Azure) => {
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
                (_, UploadMode::Single) => {
                    tracing::debug!(ingestion_id = init.ingestion_id, "uploading via single PUT");
                    self.upload_via_single(&init, file_path, total_size, Arc::clone(&progress))
                        .await?;
                }
                (_, UploadMode::Multipart) => {
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
}
