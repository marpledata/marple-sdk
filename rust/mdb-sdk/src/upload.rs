use crate::client::put_storage;
use crate::models::{Dataset, Metadata, PushFileOptions, UploadModeOverride};
use crate::{Error, MarpleDB, ProgressReporter, Result};
use base64::Engine;
use futures_util::{StreamExt, TryStreamExt};
use reqwest::{Body, header::CONTENT_LENGTH};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

impl MarpleDB {
    /// Uploads a file to a stream and returns the created dataset.
    ///
    /// Pass [`PushFileOptions::default`] for local-file defaults, or chain
    /// setters for metadata, dataset name, overwrite, concurrency, upload
    /// mode, and progress reporting.
    ///
    /// This is [`MarpleDB::begin_upload`] followed by
    /// [`UploadSession::send`]. Use those when you need the dataset row before
    /// bytes finish transferring.
    #[tracing::instrument(skip_all, fields(stream_id, path = %file_path.as_ref().display()))]
    pub async fn push_file(
        &self,
        stream_id: i64,
        file_path: impl AsRef<Path>,
        options: PushFileOptions,
    ) -> Result<Dataset> {
        self.begin_upload(stream_id, file_path, options)
            .await?
            .send()
            .await
    }

    /// Creates the dataset and returns an upload session.
    ///
    /// [`UploadSession::dataset`] is available immediately (`UPLOADING`). Call
    /// [`UploadSession::send`] to transfer bytes and complete the session.
    /// Dropping a session without `send` leaves an `UPLOADING` dataset on the
    /// server.
    #[tracing::instrument(skip_all, fields(stream_id, path = %file_path.as_ref().display()))]
    pub async fn begin_upload(
        &self,
        stream_id: i64,
        file_path: impl AsRef<Path>,
        options: PushFileOptions,
    ) -> Result<UploadSession> {
        let file_path = file_path.as_ref().to_path_buf();
        let file_name = file_name_from_path(&file_path)?;
        let total_size = tokio::fs::metadata(&file_path).await?.len();
        let init = self
            .init_ingestion(
                stream_id,
                options.dataset_name.as_deref().unwrap_or(&file_name),
                total_size,
                &options.metadata,
                options.overwrite,
            )
            .await?;
        match self.get_dataset(stream_id, init.dataset_id).await {
            Ok(dataset) => Ok(UploadSession {
                db: self.clone(),
                stream_id,
                init,
                dataset,
                file_path,
                file_name,
                total_size,
                options,
            }),
            Err(error) => {
                let _ = self
                    .abort_upload(init.ingestion_id, &format!("{error:#}"))
                    .await;
                Err(error)
            }
        }
    }
}

/// In-progress file upload created by [`MarpleDB::begin_upload`].
///
/// The dataset row exists as soon as the session is returned. [`UploadSession::send`]
/// transfers bytes and completes (or aborts) the upload.
pub struct UploadSession {
    db: MarpleDB,
    stream_id: i64,
    init: IngestionInit,
    dataset: Dataset,
    file_path: PathBuf,
    file_name: String,
    total_size: u64,
    options: PushFileOptions,
}

impl fmt::Debug for UploadSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadSession")
            .field("stream_id", &self.stream_id)
            .field("dataset_id", &self.dataset.id)
            .field("file_path", &self.file_path)
            .field("total_size", &self.total_size)
            .finish_non_exhaustive()
    }
}

impl UploadSession {
    /// Dataset created by this upload. Status is typically `UPLOADING`.
    pub fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    /// Local file size in bytes.
    pub fn total_size(&self) -> u64 {
        self.total_size
    }

    /// Transfers the file and completes the upload.
    ///
    /// On success, returns the dataset (typically `WAITING`). On failure,
    /// aborts the ingestion and returns the error.
    pub async fn send(self) -> Result<Dataset> {
        match self.upload_and_complete().await {
            Ok(dataset) => Ok(dataset),
            Err(error) => {
                let _ = self
                    .db
                    .abort_upload(self.init.ingestion_id, &format!("{error:#}"))
                    .await;
                Err(error)
            }
        }
    }

    async fn upload_and_complete(&self) -> Result<Dataset> {
        let progress = Arc::clone(&self.options.progress);
        match effective_upload_mode(self.options.upload_mode, self.init.mode) {
            UploadMode::Server => {
                tracing::debug!(
                    ingestion_id = self.init.ingestion_id,
                    "uploading via server"
                );
                self.db
                    .upload_via_server(
                        &self.init,
                        &self.file_path,
                        &self.file_name,
                        self.total_size,
                        Arc::clone(&progress),
                    )
                    .await?;
            }
            UploadMode::Azure => {
                tracing::debug!(
                    ingestion_id = self.init.ingestion_id,
                    "uploading via Azure blocks"
                );
                self.db
                    .upload_via_azure(
                        &self.init,
                        &self.file_path,
                        self.total_size,
                        self.options.concurrency,
                        Arc::clone(&progress),
                    )
                    .await?;
            }
            UploadMode::Single => {
                tracing::debug!(
                    ingestion_id = self.init.ingestion_id,
                    "uploading via single PUT"
                );
                self.db
                    .upload_via_single(
                        &self.init,
                        &self.file_path,
                        self.total_size,
                        Arc::clone(&progress),
                    )
                    .await?;
            }
            UploadMode::Multipart => {
                tracing::debug!(
                    ingestion_id = self.init.ingestion_id,
                    "uploading via multipart"
                );
                self.db
                    .upload_via_multipart(
                        &self.init,
                        &self.file_path,
                        self.total_size,
                        self.options.concurrency,
                        Arc::clone(&progress),
                    )
                    .await?;
            }
        }
        self.db.complete_upload(self.init.ingestion_id).await?;
        self.db
            .get_dataset(self.stream_id, self.init.dataset_id)
            .await
    }
}

impl MarpleDB {
    async fn upload_via_server(
        &self,
        init: &IngestionInit,
        file_path: &Path,
        file_name: &str,
        total_size: u64,
        progress: Arc<dyn ProgressReporter>,
    ) -> Result<()> {
        let body = file_progress_body(tokio::fs::File::open(file_path).await?, progress);
        let part = reqwest::multipart::Part::stream_with_length(body, total_size)
            .file_name(file_name.to_string())
            .mime_str("application/octet-stream")
            .map_err(|source| {
                Error::storage("building multipart upload body", None, None, Some(source))
            })?;
        let form = reqwest::multipart::Form::new().part("file", part);
        let ingestion_id = init.ingestion_id;
        self.post_multipart(&format!("ingestion/{ingestion_id}/upload/server"), form)
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

        let url = init
            .presigned_url
            .as_deref()
            .ok_or_else(|| Error::protocol("azure upload mode without presigned_url"))?;
        let sas_url: reqwest::Url = url.parse()?;

        let descriptors = azure_block_descriptors(total_size, AZURE_BLOCK_SIZE);
        let context = AzureBlockUploadContext {
            sas_url: Arc::new(sas_url.clone()),
            uploaded: Arc::new(AtomicU64::new(0)),
            progress: Arc::clone(&progress),
        };

        futures_util::stream::iter(descriptors.clone())
            .map(Ok)
            .try_for_each_concurrent(concurrency.max(1), |descriptor| async {
                let mut file = tokio::fs::File::open(file_path).await?;
                self.put_block(&mut file, &context, descriptor).await
            })
            .await?;

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
        let url = init
            .presigned_url
            .as_deref()
            .ok_or_else(|| Error::protocol("single upload mode without presigned_url"))?;
        let body = file_progress_body(tokio::fs::File::open(file_path).await?, progress);
        put_storage(
            self.storage_client
                .put(url)
                .header(CONTENT_LENGTH, total_size)
                .body(body),
            "storage PUT failed",
        )
        .await
    }

    async fn upload_via_multipart(
        &self,
        init: &IngestionInit,
        file_path: &Path,
        total_size: u64,
        concurrency: usize,
        progress: Arc<dyn ProgressReporter>,
    ) -> Result<()> {
        let part_size = init
            .part_size
            .ok_or_else(|| Error::protocol("multipart upload mode without part_size"))?;
        if part_size == 0 {
            return Err(Error::protocol(
                "multipart upload part_size must be positive",
            ));
        }

        let context = MultipartUploadContext {
            part_size,
            total_size,
            uploaded: Arc::new(AtomicU64::new(0)),
            progress: Arc::clone(&progress),
        };

        self.signed_parts_stream(init.ingestion_id, concurrency.max(32))
            .try_for_each_concurrent(concurrency.max(1), |part| async {
                let mut file = tokio::fs::File::open(file_path).await?;
                self.put_part(&mut file, &context, part).await
            })
            .await?;

        progress.finish();
        Ok(())
    }

    async fn init_ingestion(
        &self,
        stream_id: i64,
        dataset_name: &str,
        file_size: u64,
        metadata: &Metadata,
        overwrite: bool,
    ) -> Result<IngestionInit> {
        self.post(
            "ingestion",
            &IngestionRequest {
                stream_id,
                dataset_name,
                file_size,
                metadata,
                overwrite,
            },
        )
        .await
    }

    async fn complete_upload(&self, ingestion_id: i64) -> Result<()> {
        self.post::<_, Value>(
            &format!("ingestion/{ingestion_id}/upload/complete"),
            &serde_json::json!({}),
        )
        .await?;
        Ok(())
    }

    async fn abort_upload(&self, ingestion_id: i64, reason: &str) -> Result<()> {
        self.post::<_, Value>(
            &format!("ingestion/{ingestion_id}/abort"),
            &serde_json::json!({ "reason": reason }),
        )
        .await?;
        Ok(())
    }

    async fn put_block(
        &self,
        file: &mut tokio::fs::File,
        context: &AzureBlockUploadContext,
        descriptor: BlockDescriptor,
    ) -> Result<()> {
        let data = read_file_range(file, descriptor.offset, descriptor.length).await?;

        let mut block_url = (*context.sas_url).clone();
        block_url
            .query_pairs_mut()
            .append_pair("comp", "block")
            .append_pair(
                "blockid",
                &base64::engine::general_purpose::STANDARD.encode(descriptor.block_id.as_bytes()),
            );

        self.put_bytes(
            block_url,
            data,
            &context.uploaded,
            context.progress.as_ref(),
            format!("Azure block {} upload failed", descriptor.block_id),
        )
        .await
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

        put_storage(
            self.storage_client
                .put(block_list_url)
                .header(reqwest::header::CONTENT_TYPE, "application/xml")
                .header(reqwest::header::CONTENT_LENGTH, xml.len())
                .header("x-ms-date", date)
                .header("x-ms-version", "2022-11-02")
                .body(xml),
            "Azure block list commit failed",
        )
        .await
    }

    async fn put_part(
        &self,
        file: &mut tokio::fs::File,
        context: &MultipartUploadContext,
        part: PartUrl,
    ) -> Result<()> {
        let (offset, part_len) =
            part_byte_range(part.part_number, context.part_size, context.total_size)?;
        let data = read_file_range(file, offset, part_len).await?;
        self.put_bytes(
            part.url,
            data,
            &context.uploaded,
            context.progress.as_ref(),
            format!("part {} storage PUT failed", part.part_number),
        )
        .await
    }

    async fn put_bytes(
        &self,
        url: impl reqwest::IntoUrl,
        data: Vec<u8>,
        uploaded: &AtomicU64,
        progress: &dyn ProgressReporter,
        context: impl Into<String>,
    ) -> Result<()> {
        let length = data.len() as u64;
        progress.set_position(uploaded.fetch_add(length, Ordering::Relaxed) + length);
        put_storage(
            self.storage_client
                .put(url)
                .header(CONTENT_LENGTH, length)
                .body(data),
            context,
        )
        .await
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
                    Err(Error::protocol("server returned no multipart upload URLs"))?;
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
        self.get(
            &format!("ingestion/{ingestion_id}/upload/part-urls"),
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
}

#[derive(Serialize)]
struct IngestionRequest<'a> {
    stream_id: i64,
    dataset_name: &'a str,
    file_size: u64,
    metadata: &'a Metadata,
    overwrite: bool,
}

#[derive(Debug, Deserialize)]
struct PartUrl {
    part_number: u32,
    url: String,
}

#[derive(Debug, Deserialize)]
struct PartUrlsResponse {
    parts: Vec<PartUrl>,
    next_part: Option<u32>,
}

#[derive(Clone)]
struct MultipartUploadContext {
    part_size: u64,
    total_size: u64,
    uploaded: Arc<AtomicU64>,
    progress: Arc<dyn ProgressReporter>,
}

#[derive(Clone)]
struct AzureBlockUploadContext {
    sas_url: Arc<reqwest::Url>,
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
            Error::config(format!(
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
        return Err(Error::protocol("multipart part numbers are 1-based"));
    }
    let offset = u64::from(part_number - 1) * part_size;
    if offset >= total_size {
        return Err(Error::protocol(format!(
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

fn file_progress_body(file: tokio::fs::File, progress: Arc<dyn ProgressReporter>) -> Body {
    let mut uploaded = 0u64;
    let mut reader = ReaderStream::new(file);
    Body::wrap_stream(async_stream::stream! {
        while let Some(chunk) = reader.next().await {
            if let Ok(chunk) = &chunk {
                uploaded += chunk.len() as u64;
                progress.set_position(uploaded);
            }
            yield chunk;
        }
        progress.finish();
    })
}

async fn read_file_range(file: &mut tokio::fs::File, offset: u64, length: u64) -> Result<Vec<u8>> {
    let len = usize::try_from(length).map_err(|_| {
        Error::protocol(format!(
            "upload chunk of {length} bytes does not fit in memory"
        ))
    })?;
    file.seek(SeekFrom::Start(offset)).await?;
    let mut data = vec![0; len];
    file.read_exact(&mut data).await?;
    Ok(data)
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
