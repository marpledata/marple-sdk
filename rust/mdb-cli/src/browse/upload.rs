use super::picker::FilePicker;
use super::{App, BrowseLevel, Focus};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use marple_db::{
    Dataset, DatasetStatus, ImportStatus, ProgressReporter, PushFileOptions, StreamType,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::{Duration, Instant};
use walkdir::WalkDir;

const STATUS_POLL: Duration = Duration::from_millis(500);

#[derive(Default)]
pub(super) struct UploadState {
    pub form: Option<UploadForm>,
    queue: VecDeque<QueuedFile>,
    running: Option<RunningUpload>,
    watch: Vec<Watch>,
    last_poll: Option<Instant>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum FormFocus {
    Options,
    Files,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum OptionField {
    Overwrite,
    SkipExisting,
    Extension,
}

pub(super) struct UploadForm {
    pub overwrite: bool,
    pub skip_existing: bool,
    pub extension: String,
    pub focus: FormFocus,
    pub option: OptionField,
    pub picker: FilePicker,
    pub stream_name: String,
    stream_id: i64,
}

struct QueuedFile {
    stream_id: i64,
    path: PathBuf,
    overwrite: bool,
}

struct RunningUpload {
    dataset_id: Option<i64>,
    bytes: Arc<AtomicU64>,
    total: u64,
    rx: Receiver<UploadEvent>,
}

struct Watch {
    stream_id: i64,
    dataset_id: i64,
}

enum UploadEvent {
    Created(Dataset),
    Finished(Result<Dataset, String>),
}

impl UploadState {
    pub(super) fn needs_tick(&self) -> bool {
        self.running.is_some() || !self.queue.is_empty() || !self.watch.is_empty()
    }

    pub(super) fn byte_ratio(&self, dataset_id: i64) -> Option<f64> {
        let running = self.running.as_ref()?;
        if running.dataset_id != Some(dataset_id) {
            return None;
        }
        if running.total == 0 {
            return Some(1.0);
        }
        Some((running.bytes.load(Ordering::Relaxed) as f64 / running.total as f64).clamp(0.0, 1.0))
    }
}

impl OptionField {
    fn next(self) -> Self {
        match self {
            Self::Overwrite => Self::SkipExisting,
            Self::SkipExisting => Self::Extension,
            Self::Extension => Self::Overwrite,
        }
    }

    fn prev(self) -> Self {
        match self {
            Self::Overwrite => Self::Extension,
            Self::SkipExisting => Self::Overwrite,
            Self::Extension => Self::SkipExisting,
        }
    }
}

impl App {
    pub(super) fn open_upload(&mut self) {
        if self.upload.form.is_some() {
            return;
        }
        if !self.connected {
            self.prompt_for_env();
            return;
        }
        let Some(stream) = self.selected_stream() else {
            self.status = "select a stream first".to_string();
            return;
        };
        if stream.stream_type != StreamType::Files {
            self.status = "upload is only available on file streams".to_string();
            return;
        }
        let stream_id = stream.id;
        let stream_name = stream.name.clone();
        if self.browse_level == BrowseLevel::Root {
            self.open_stream_table();
        } else {
            self.request_datasets(stream_id);
            self.browse_level = BrowseLevel::Streams;
            self.focus = Focus::Table;
        }
        self.status.clear();
        self.upload.form = Some(UploadForm::new(stream_id, stream_name));
    }

    pub(super) async fn on_upload_tick(&mut self) {
        self.apply_upload_events();
        self.patch_upload_progress();
        self.start_next_upload();
        self.poll_import_statuses().await;
    }

    pub(super) fn handle_upload_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Esc => {
                self.upload.form = None;
                return false;
            }
            KeyCode::Char('q')
                if self
                    .upload
                    .form
                    .as_ref()
                    .is_none_or(|form| form.option != OptionField::Extension) =>
            {
                self.upload.form = None;
                return false;
            }
            _ => {}
        }
        let Some(form) = self.upload.form.as_mut() else {
            return false;
        };
        match key.code {
            KeyCode::Tab | KeyCode::BackTab => {
                form.focus = match form.focus {
                    FormFocus::Options => FormFocus::Files,
                    FormFocus::Files => {
                        form.picker.cancel_editing();
                        FormFocus::Options
                    }
                };
            }
            KeyCode::Enter if form.focus == FormFocus::Options => form.toggle_option(),
            KeyCode::Enter => {
                if let Some(path) = form.picker.take_selected(true) {
                    self.queue_upload_path(path);
                }
            }
            KeyCode::Char(' ') if form.focus == FormFocus::Options => form.toggle_option(),
            KeyCode::Char('/') if form.focus == FormFocus::Files => form.picker.start_editing(),
            KeyCode::Char('h') | KeyCode::Left if form.focus == FormFocus::Options => {
                form.option = form.option.prev();
            }
            KeyCode::Char('l') | KeyCode::Right if form.focus == FormFocus::Options => {
                form.option = form.option.next();
            }
            KeyCode::Char('h') | KeyCode::Left | KeyCode::Backspace
                if form.focus == FormFocus::Files =>
            {
                form.picker.go_parent();
            }
            KeyCode::Char('l') | KeyCode::Right
                if form.focus == FormFocus::Files
                    && form
                        .picker
                        .selected_entry()
                        .is_some_and(|entry| entry.is_dir) =>
            {
                form.picker.enter_selected();
            }
            KeyCode::Backspace
                if form.focus == FormFocus::Options && form.option == OptionField::Extension =>
            {
                form.extension.pop();
            }
            KeyCode::Char(ch)
                if form.focus == FormFocus::Options
                    && form.option == OptionField::Extension
                    && !key.modifiers.contains(KeyModifiers::CONTROL)
                    && (ch.is_ascii_alphanumeric() || ch == '.') =>
            {
                form.extension.push(ch);
            }
            _ => {}
        }
        false
    }

    pub(super) async fn handle_upload_input(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Enter => {
                let result = self
                    .upload
                    .form
                    .as_mut()
                    .map(|form| form.picker.submit_typed(true))
                    .unwrap_or(Err("no picker".to_string()));
                match result {
                    Ok(Some(path)) => self.queue_upload_path(path),
                    Ok(None) => {}
                    Err(error) => self.status = error,
                }
            }
            other => {
                let Some(form) = self.upload.form.as_mut() else {
                    return false;
                };
                match other {
                    KeyCode::Esc => form.picker.cancel_editing(),
                    KeyCode::Backspace => form.picker.backspace(),
                    KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                        form.picker.push_char(ch);
                    }
                    _ => {}
                }
            }
        }
        false
    }

    pub(super) fn apply_upload_motion(&mut self, motion: super::Motion) {
        let Some(form) = self.upload.form.as_mut() else {
            return;
        };
        match form.focus {
            FormFocus::Options => match motion {
                super::Motion::Delta(delta) if delta > 0 => form.option = form.option.next(),
                super::Motion::Delta(_) => form.option = form.option.prev(),
                _ => {}
            },
            FormFocus::Files => {
                let last = form.picker.len().saturating_sub(1);
                match motion {
                    super::Motion::Delta(delta) => form.picker.move_sel(delta),
                    super::Motion::Page(pages) => form.picker.move_sel(pages * super::PAGE_SIZE),
                    super::Motion::First => form.picker.goto(0),
                    super::Motion::Last => form.picker.goto(last),
                    super::Motion::Goto(index) => form.picker.goto(index),
                }
            }
        }
    }

    fn queue_upload_path(&mut self, path: PathBuf) {
        let Some(form) = self.upload.form.take() else {
            return;
        };
        let extension = form.extension.trim();
        let extension = (!extension.is_empty()).then_some(extension);
        let files = match collect_files(&path, extension) {
            Ok(files) => files,
            Err(error) => {
                self.status = error;
                self.upload.form = Some(form);
                return;
            }
        };
        if files.is_empty() {
            self.status = "no files to upload".to_string();
            self.upload.form = Some(form);
            return;
        }
        let existing: HashSet<String> = if form.skip_existing {
            self.datasets
                .iter()
                .map(|dataset| dataset.path.clone())
                .collect()
        } else {
            HashSet::new()
        };
        let mut queued = 0usize;
        for file in files {
            let name = file
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            if form.skip_existing && existing.contains(&name) {
                continue;
            }
            self.upload.queue.push_back(QueuedFile {
                stream_id: form.stream_id,
                path: file,
                overwrite: form.overwrite,
            });
            queued += 1;
        }
        if queued == 0 {
            self.status = "all files already exist".to_string();
            return;
        }
        self.search.clear();
        self.status.clear();
        self.start_next_upload();
    }

    fn start_next_upload(&mut self) {
        if self.upload.running.is_some() {
            return;
        }
        let Some(front) = self.upload.queue.front() else {
            return;
        };
        if self.loaded_stream_id != Some(front.stream_id) {
            self.request_datasets(front.stream_id);
            return;
        }
        let queued = self.upload.queue.pop_front().expect("front");
        let total = std::fs::metadata(&queued.path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let bytes = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::channel();
        let db = self.db.clone();
        let progress = Arc::new(AtomicProgress(Arc::clone(&bytes)));
        tokio::spawn(async move {
            let options = PushFileOptions::default()
                .overwrite(queued.overwrite)
                .progress(progress);
            match db
                .begin_upload(queued.stream_id, &queued.path, options)
                .await
            {
                Ok(session) => {
                    let _ = tx.send(UploadEvent::Created(session.dataset().clone()));
                    let result = session.send().await.map_err(|error| error.to_string());
                    let _ = tx.send(UploadEvent::Finished(result));
                }
                Err(error) => {
                    let _ = tx.send(UploadEvent::Finished(Err(error.to_string())));
                }
            }
        });
        self.upload.running = Some(RunningUpload {
            dataset_id: None,
            bytes,
            total,
            rx,
        });
    }

    fn apply_upload_events(&mut self) {
        loop {
            let event = {
                let Some(running) = self.upload.running.as_mut() else {
                    return;
                };
                match running.rx.try_recv() {
                    Ok(event) => event,
                    Err(TryRecvError::Empty) => return,
                    Err(TryRecvError::Disconnected) => {
                        self.status = "upload task ended unexpectedly".to_string();
                        self.upload.running = None;
                        return;
                    }
                }
            };
            match event {
                UploadEvent::Created(dataset) => {
                    if let Some(running) = self.upload.running.as_mut() {
                        running.dataset_id = Some(dataset.id);
                    }
                    self.upsert_dataset(dataset);
                }
                UploadEvent::Finished(Ok(dataset)) => {
                    let stream_id = dataset.datastream_id;
                    let dataset_id = dataset.id;
                    self.upsert_dataset(dataset);
                    self.upload.watch.push(Watch {
                        stream_id,
                        dataset_id,
                    });
                    self.upload.running = None;
                    return;
                }
                UploadEvent::Finished(Err(error)) => {
                    if let Some(id) = self
                        .upload
                        .running
                        .as_ref()
                        .and_then(|running| running.dataset_id)
                        && let Some(dataset) =
                            self.datasets.iter_mut().find(|dataset| dataset.id == id)
                    {
                        dataset.import_status = ImportStatus::Failed;
                        dataset.import_message = Some(error.clone());
                    }
                    self.status = error;
                    self.upload.running = None;
                    return;
                }
            }
        }
    }

    fn patch_upload_progress(&mut self) {
        let Some(running) = &self.upload.running else {
            return;
        };
        let Some(id) = running.dataset_id else {
            return;
        };
        let ratio = self.upload.byte_ratio(id);
        if let Some(dataset) = self.datasets.iter_mut().find(|dataset| dataset.id == id) {
            dataset.import_status = ImportStatus::Uploading;
            dataset.import_progress = ratio;
        }
    }

    async fn poll_import_statuses(&mut self) {
        if self.upload.watch.is_empty() {
            return;
        }
        if self
            .upload
            .last_poll
            .is_some_and(|last| last.elapsed() < STATUS_POLL)
        {
            return;
        }
        self.upload.last_poll = Some(Instant::now());
        let mut by_stream: HashMap<i64, Vec<i64>> = HashMap::new();
        for watch in &self.upload.watch {
            by_stream
                .entry(watch.stream_id)
                .or_default()
                .push(watch.dataset_id);
        }
        let db = self.db.clone();
        for (stream_id, ids) in by_stream {
            match db.get_dataset_statuses(stream_id, &ids).await {
                Ok(statuses) => self.apply_status_batch(stream_id, statuses).await,
                Err(error) => self.status = error.to_string(),
            }
        }
    }

    async fn apply_status_batch(&mut self, stream_id: i64, statuses: Vec<DatasetStatus>) {
        let mut done = Vec::new();
        for status in statuses {
            self.patch_dataset_status(&status);
            if !status.import_status.is_terminal() {
                continue;
            }
            done.push(status.dataset_id);
            if status.import_status.is_success()
                && let Ok(dataset) = self.db.get_dataset(stream_id, status.dataset_id).await
            {
                self.upsert_dataset(dataset);
            }
        }
        self.upload
            .watch
            .retain(|watch| !(watch.stream_id == stream_id && done.contains(&watch.dataset_id)));
    }

    fn patch_dataset_status(&mut self, status: &DatasetStatus) {
        let Some(dataset) = self
            .datasets
            .iter_mut()
            .find(|dataset| dataset.id == status.dataset_id)
        else {
            return;
        };
        dataset.import_status = status.import_status;
        dataset.import_progress = status.import_progress;
        dataset.import_message = status.import_message.clone();
    }

    fn upsert_dataset(&mut self, dataset: Dataset) {
        if self.loaded_stream_id.is_some() && self.loaded_stream_id != Some(dataset.datastream_id) {
            return;
        }
        if let Some(index) = self.datasets.iter().position(|row| row.id == dataset.id) {
            self.datasets[index] = dataset;
            return;
        }
        self.datasets.insert(0, dataset);
        self.dataset_state.select(Some(0));
    }
}

impl UploadForm {
    fn new(stream_id: i64, stream_name: String) -> Self {
        Self {
            overwrite: false,
            skip_existing: false,
            extension: String::new(),
            focus: FormFocus::Files,
            option: OptionField::Overwrite,
            picker: FilePicker::open(None, &[]),
            stream_name,
            stream_id,
        }
    }

    fn toggle_option(&mut self) {
        match self.option {
            OptionField::Overwrite => {
                self.overwrite = !self.overwrite;
                if self.overwrite {
                    self.skip_existing = false;
                }
            }
            OptionField::SkipExisting => {
                self.skip_existing = !self.skip_existing;
                if self.skip_existing {
                    self.overwrite = false;
                }
            }
            OptionField::Extension => {}
        }
    }
}

struct AtomicProgress(Arc<AtomicU64>);

impl ProgressReporter for AtomicProgress {
    fn set_position(&self, position: u64) {
        self.0.store(position, Ordering::Relaxed);
    }

    fn finish(&self) {}
}

fn collect_files(path: &Path, extension: Option<&str>) -> Result<Vec<PathBuf>, String> {
    let matches_ext = |candidate: &Path| {
        extension.is_none_or(|ext| {
            candidate.extension().is_some_and(|path_ext| {
                path_ext
                    .to_string_lossy()
                    .eq_ignore_ascii_case(ext.trim_start_matches('.'))
            })
        })
    };
    if path.is_file() {
        if matches_ext(path) {
            return Ok(vec![path.to_path_buf()]);
        }
        return Err(format!(
            "{} skipped (extension)",
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| path.display().to_string())
        ));
    }
    if path.is_dir() {
        let mut files: Vec<PathBuf> = WalkDir::new(path)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file() && matches_ext(entry.path()))
            .map(|entry| entry.into_path())
            .collect();
        files.sort();
        return Ok(files);
    }
    Err(format!("{} is not a file or folder", path.display()))
}

#[cfg(test)]
mod tests {
    use super::collect_files;
    use std::fs;

    #[test]
    fn collect_files_walks_a_folder_and_filters_extension() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("nested")).unwrap();
        fs::write(tmp.path().join("run.csv"), "a").unwrap();
        fs::write(tmp.path().join("nested/inner.csv"), "b").unwrap();
        fs::write(tmp.path().join("notes.txt"), "c").unwrap();
        let files = collect_files(tmp.path(), Some("csv")).unwrap();
        assert_eq!(files.len(), 2);
        assert!(
            files
                .iter()
                .all(|path| path.extension().and_then(|ext| ext.to_str()) == Some("csv"))
        );
    }
}
