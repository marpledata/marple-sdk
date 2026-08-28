use super::picker::FilePicker;
use super::{App, BrowseLevel, Focus};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use marple_db::{
    Dataset, DatasetStatus, ImportStatus, Metadata, ProgressReporter, PushFileOptions, StreamType,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::{Duration, Instant};
use walkdir::WalkDir;

const STATUS_POLL: Duration = Duration::from_millis(500);
const FOOTER: [FormFocus; 5] = [
    FormFocus::Overwrite,
    FormFocus::SkipExisting,
    FormFocus::Extension,
    FormFocus::Metadata,
    FormFocus::Upload,
];

#[derive(Default)]
pub(super) struct UploadState {
    pub form: Option<UploadForm>,
    queue: VecDeque<QueuedFile>,
    running: Option<RunningUpload>,
    watch: Vec<Watch>,
    last_poll: Option<Instant>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FormFocus {
    Files,
    Overwrite,
    SkipExisting,
    Extension,
    Metadata,
    Upload,
}

pub(super) struct UploadForm {
    pub overwrite: bool,
    pub skip_existing: bool,
    pub extension: String,
    pub ext_editing: bool,
    pub metadata: Vec<(String, Value)>,
    pub meta_input: String,
    pub meta_editing: bool,
    pub focus: FormFocus,
    pub picker: FilePicker,
    pub stream_name: String,
    pub selected: HashSet<PathBuf>,
    stream_id: i64,
}

struct QueuedFile {
    stream_id: i64,
    path: PathBuf,
    overwrite: bool,
    metadata: Metadata,
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
    pub(super) fn clear(&mut self) {
        self.watch.clear();
        self.queue.clear();
    }

    pub(super) fn needs_tick(&self) -> bool {
        self.running.is_some() || !self.queue.is_empty() || !self.watch.is_empty()
    }

    pub(super) fn is_active(&self, dataset_id: i64) -> bool {
        self.running
            .as_ref()
            .is_some_and(|running| running.dataset_id == Some(dataset_id))
            || self
                .watch
                .iter()
                .any(|watch| watch.dataset_id == dataset_id)
    }

    pub(super) fn watch_dataset(&mut self, stream_id: i64, dataset_id: i64) {
        if self
            .watch
            .iter()
            .any(|watch| watch.dataset_id == dataset_id)
        {
            return;
        }
        self.watch.push(Watch {
            stream_id,
            dataset_id,
        });
    }

    pub(super) fn seed_watch(&mut self, stream_id: i64, datasets: &[Dataset]) {
        self.watch.retain(|watch| watch.stream_id != stream_id);
        for dataset in datasets {
            if !dataset.import_status.is_terminal() {
                self.watch.push(Watch {
                    stream_id,
                    dataset_id: dataset.id,
                });
            }
        }
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

impl FormFocus {
    fn next_footer(self) -> Self {
        Self::step_footer(self, 1)
    }

    fn prev_footer(self) -> Self {
        Self::step_footer(self, FOOTER.len() - 1)
    }

    fn step_footer(self, delta: usize) -> Self {
        let index = FOOTER.iter().position(|&field| field == self).unwrap_or(0);
        FOOTER[(index + delta) % FOOTER.len()]
    }

    pub(super) fn is_files(self) -> bool {
        matches!(self, Self::Files)
    }
}

impl App {
    pub(super) fn open_upload(&mut self) {
        if self.upload.form.is_some() || self.download.picker.is_some() {
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
        self.upload.form = Some(UploadForm::new(
            stream_id,
            stream_name,
            self.upload_dir.as_deref(),
        ));
    }

    pub(super) fn upload_typing(&self) -> bool {
        self.upload.form.as_ref().is_some_and(UploadForm::typing)
    }

    pub(super) async fn on_upload_tick(&mut self) {
        self.apply_upload_events();
        self.patch_upload_progress();
        self.start_next_upload();
        self.poll_import_statuses().await;
    }

    pub(super) fn handle_upload_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                if self.cancel_upload_edit() {
                    return;
                }
                self.close_upload();
                return;
            }
            KeyCode::Char('q') if !self.upload_typing() => {
                self.close_upload();
                return;
            }
            _ => {}
        }
        let confirm = {
            let Some(form) = self.upload.form.as_mut() else {
                return;
            };
            let confirm = form.focus == FormFocus::Upload && matches!(key.code, KeyCode::Enter);
            match key.code {
                KeyCode::Tab => {
                    form.picker.cancel_editing();
                    form.stop_edits();
                    form.focus = if form.focus.is_files() {
                        FormFocus::Overwrite
                    } else {
                        FormFocus::Files
                    };
                }
                KeyCode::BackTab => {
                    form.picker.cancel_editing();
                    form.stop_edits();
                    form.focus = if form.focus.is_files() {
                        FormFocus::Upload
                    } else {
                        FormFocus::Files
                    };
                }
                KeyCode::Enter if !confirm => form.activate(),
                KeyCode::Char(' ') if form.focus.is_files() || form.is_checkbox() => {
                    form.activate()
                }
                KeyCode::Char('a') if form.focus.is_files() => form.toggle_all(),
                KeyCode::Char('/') if form.focus.is_files() => form.picker.start_editing(),
                KeyCode::Char('h') | KeyCode::Left if !form.focus.is_files() => {
                    form.focus = form.focus.prev_footer();
                }
                KeyCode::Char('l') | KeyCode::Right if !form.focus.is_files() => {
                    form.focus = form.focus.next_footer();
                }
                KeyCode::Char('h') | KeyCode::Left | KeyCode::Backspace
                    if form.focus.is_files() =>
                {
                    form.picker.go_parent();
                }
                KeyCode::Char('l') | KeyCode::Right
                    if form.focus.is_files()
                        && form
                            .picker
                            .selected_entry()
                            .is_some_and(|entry| entry.is_dir) =>
                {
                    form.picker.enter_selected();
                }
                KeyCode::Backspace if form.focus == FormFocus::Metadata && !form.meta_editing => {
                    form.metadata.pop();
                }
                _ => {}
            }
            confirm
        };
        if confirm {
            self.confirm_upload();
        }
    }

    pub(super) fn handle_upload_input(&mut self, key: KeyEvent) {
        let mut status = None;
        {
            let Some(form) = self.upload.form.as_mut() else {
                return;
            };
            if form.picker.editing {
                match key.code {
                    KeyCode::Enter => match form.picker.submit_typed(true) {
                        Ok(Some(path)) => {
                            form.selected.insert(super::picker::path_key(&path));
                        }
                        Ok(None) => {}
                        Err(error) => status = Some(error),
                    },
                    KeyCode::Esc => form.picker.cancel_editing(),
                    KeyCode::Backspace => form.picker.backspace(),
                    KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                        form.picker.push_char(ch);
                    }
                    _ => {}
                }
            } else if form.ext_editing {
                match key.code {
                    KeyCode::Enter | KeyCode::Esc => form.ext_editing = false,
                    KeyCode::Backspace => {
                        form.extension.pop();
                    }
                    KeyCode::Char(ch)
                        if !key.modifiers.contains(KeyModifiers::CONTROL)
                            && (ch.is_ascii_alphanumeric() || ch == '.') =>
                    {
                        form.extension.push(ch);
                    }
                    _ => {}
                }
            } else if form.meta_editing {
                match key.code {
                    KeyCode::Enter => match parse_meta(&form.meta_input) {
                        Ok(pair) => {
                            form.metadata.retain(|(key, _)| key != &pair.0);
                            form.metadata.push(pair);
                            form.meta_input.clear();
                            form.meta_editing = false;
                        }
                        Err(error) => status = Some(error),
                    },
                    KeyCode::Esc => {
                        form.meta_input.clear();
                        form.meta_editing = false;
                    }
                    KeyCode::Backspace => {
                        form.meta_input.pop();
                    }
                    KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                        form.meta_input.push(ch);
                    }
                    _ => {}
                }
            }
        }
        if let Some(error) = status {
            self.status = error;
        }
    }

    pub(super) fn apply_upload_motion(&mut self, motion: super::Motion) {
        let Some(form) = self.upload.form.as_mut() else {
            return;
        };
        if form.focus.is_files() {
            let last = form.picker.len().saturating_sub(1);
            match motion {
                super::Motion::Delta(delta) => form.picker.move_sel(delta),
                super::Motion::Page(pages) => form.picker.move_sel(pages * super::PAGE_SIZE),
                super::Motion::First => form.picker.goto(0),
                super::Motion::Last => form.picker.goto(last),
                super::Motion::Goto(index) => form.picker.goto(index),
            }
            return;
        }
        match motion {
            super::Motion::Delta(delta) if delta > 0 => form.focus = form.focus.next_footer(),
            super::Motion::Delta(_) => form.focus = form.focus.prev_footer(),
            _ => {}
        }
    }

    fn cancel_upload_edit(&mut self) -> bool {
        let Some(form) = self.upload.form.as_mut() else {
            return false;
        };
        if form.picker.editing {
            form.picker.cancel_editing();
            return true;
        }
        if form.ext_editing {
            form.ext_editing = false;
            return true;
        }
        if form.meta_editing {
            form.meta_input.clear();
            form.meta_editing = false;
            return true;
        }
        false
    }

    fn close_upload(&mut self) {
        self.remember_upload_dir();
        self.upload.form = None;
        self.persist_settings();
    }

    fn remember_upload_dir(&mut self) {
        if let Some(form) = &self.upload.form {
            self.upload_dir = Some(form.picker.dir.clone());
        }
    }

    fn confirm_upload(&mut self) {
        let Some(form) = self.upload.form.as_ref() else {
            return;
        };
        if form.selected.is_empty() {
            self.status = "enter to select files, then tab to upload".to_string();
            return;
        }
        let paths: Vec<PathBuf> = form.selected.iter().cloned().collect();
        self.remember_upload_dir();
        self.queue_upload_paths(paths);
        self.persist_settings();
    }

    fn queue_upload_paths(&mut self, paths: Vec<PathBuf>) {
        let Some(form) = self.upload.form.as_ref() else {
            return;
        };
        let extension = form.extension.trim().to_string();
        let extension = (!extension.is_empty()).then_some(extension.as_str());
        let skip_existing = form.skip_existing;
        let overwrite = form.overwrite;
        let stream_id = form.stream_id;
        let metadata: Metadata = form.metadata.iter().cloned().collect();
        let mut files = Vec::new();
        for path in &paths {
            match collect_files(path, extension) {
                Ok(found) => files.extend(found),
                Err(error) => {
                    self.status = error;
                    return;
                }
            }
        }
        if files.is_empty() {
            self.status = "no files to upload".to_string();
            return;
        }
        let existing: HashSet<String> = if skip_existing {
            self.datasets()
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
            if skip_existing && existing.contains(&name) {
                continue;
            }
            self.upload.queue.push_back(QueuedFile {
                stream_id,
                path: file,
                overwrite,
                metadata: metadata.clone(),
            });
            queued += 1;
        }
        if queued == 0 {
            self.status = "all files already exist".to_string();
            return;
        }
        self.upload.form = None;
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
        if self.loaded_stream_id() != Some(front.stream_id) {
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
                .metadata(queued.metadata)
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
                    {
                        self.patch_dataset(id, |dataset| {
                            dataset.import_status = ImportStatus::Failed;
                            dataset.import_message = Some(error.clone());
                        });
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
        self.patch_dataset(id, |dataset| {
            dataset.import_status = ImportStatus::Uploading;
            dataset.import_progress = ratio;
        });
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
        self.patch_dataset(status.dataset_id, |dataset| {
            dataset.import_status = status.import_status;
            dataset.import_progress = status.import_progress;
            dataset.import_message = status.import_message.clone();
        });
    }
}

impl UploadForm {
    fn new(stream_id: i64, stream_name: String, start: Option<&Path>) -> Self {
        Self {
            overwrite: false,
            skip_existing: false,
            extension: String::new(),
            ext_editing: false,
            metadata: Vec::new(),
            meta_input: String::new(),
            meta_editing: false,
            focus: FormFocus::Files,
            picker: FilePicker::open(start, &[]),
            stream_name,
            selected: HashSet::new(),
            stream_id,
        }
    }

    fn typing(&self) -> bool {
        self.picker.editing || self.ext_editing || self.meta_editing
    }

    fn is_checkbox(&self) -> bool {
        matches!(self.focus, FormFocus::Overwrite | FormFocus::SkipExisting)
    }

    fn stop_edits(&mut self) {
        self.ext_editing = false;
        self.meta_editing = false;
        self.meta_input.clear();
    }

    fn activate(&mut self) {
        match self.focus {
            FormFocus::Files => self.toggle_pick(),
            FormFocus::Overwrite => {
                self.overwrite = !self.overwrite;
                if self.overwrite {
                    self.skip_existing = false;
                }
            }
            FormFocus::SkipExisting => {
                self.skip_existing = !self.skip_existing;
                if self.skip_existing {
                    self.overwrite = false;
                }
            }
            FormFocus::Extension => self.ext_editing = !self.ext_editing,
            FormFocus::Metadata => {
                if self.meta_editing {
                    return;
                }
                self.meta_editing = true;
                self.meta_input.clear();
            }
            FormFocus::Upload => {}
        }
    }

    fn toggle_pick(&mut self) {
        let Some(entry) = self.picker.selected_entry() else {
            return;
        };
        if entry.name == ".." {
            return;
        }
        let key = super::picker::path_key(&entry.path);
        if !self.selected.remove(&key) {
            self.selected.insert(key);
        }
    }

    fn toggle_all(&mut self) {
        let keys: Vec<PathBuf> = self
            .picker
            .entries
            .iter()
            .filter(|entry| entry.name != "..")
            .map(|entry| super::picker::path_key(&entry.path))
            .collect();
        if keys.is_empty() {
            return;
        }
        let all_selected = keys.iter().all(|key| self.selected.contains(key));
        if all_selected {
            for key in keys {
                self.selected.remove(&key);
            }
        } else {
            self.selected.extend(keys);
        }
    }
}

pub(super) fn selected_summary(selected: &HashSet<PathBuf>) -> String {
    let mut files = 0usize;
    let mut folders = 0usize;
    for path in selected {
        if path.is_dir() {
            folders += 1;
        } else {
            files += 1;
        }
    }
    match (files, folders) {
        (0, 0) => String::new(),
        (files, 0) => format!("{files} file{}", if files == 1 { "" } else { "s" }),
        (0, folders) => format!("{folders} folder{}", if folders == 1 { "" } else { "s" }),
        (files, folders) => format!(
            "{files} file{}, {folders} folder{}",
            if files == 1 { "" } else { "s" },
            if folders == 1 { "" } else { "s" }
        ),
    }
}

struct AtomicProgress(Arc<AtomicU64>);

impl ProgressReporter for AtomicProgress {
    fn set_position(&self, position: u64) {
        self.0.store(position, Ordering::Relaxed);
    }

    fn finish(&self) {}
}

fn parse_meta(input: &str) -> Result<(String, Value), String> {
    let Some((key, value)) = input.split_once('=') else {
        return Err("metadata needs key=value".to_string());
    };
    let key = key.trim();
    if key.is_empty() {
        return Err("metadata key is empty".to_string());
    }
    let value = value.trim();
    let parsed = serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()));
    Ok((key.to_string(), parsed))
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
    use super::{FormFocus, UploadForm, UploadState, collect_files, parse_meta, selected_summary};
    use marple_db::Dataset;
    use serde_json::json;
    use std::collections::HashSet;
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

    #[test]
    fn seed_watch_follows_non_terminal_datasets() {
        let waiting: Dataset = serde_json::from_value(json!({
            "id": 1,
            "datastream_id": 7,
            "path": "a.csv",
            "import_status": "WAITING"
        }))
        .unwrap();
        let finished: Dataset = serde_json::from_value(json!({
            "id": 2,
            "datastream_id": 7,
            "path": "b.csv",
            "import_status": "FINISHED"
        }))
        .unwrap();
        let mut state = UploadState::default();
        state.seed_watch(7, &[waiting, finished]);
        assert!(state.is_active(1));
        assert!(!state.is_active(2));
        assert!(state.needs_tick());
        state.clear();
        assert!(!state.needs_tick());
    }

    #[test]
    fn footer_focus_cycles_through_upload() {
        assert_eq!(FormFocus::Overwrite.next_footer(), FormFocus::SkipExisting);
        assert_eq!(FormFocus::SkipExisting.next_footer(), FormFocus::Extension);
        assert_eq!(FormFocus::Extension.next_footer(), FormFocus::Metadata);
        assert_eq!(FormFocus::Metadata.next_footer(), FormFocus::Upload);
        assert_eq!(FormFocus::Upload.next_footer(), FormFocus::Overwrite);
        assert_eq!(FormFocus::Upload.prev_footer(), FormFocus::Metadata);
        assert!(FormFocus::Files.is_files());
        assert!(!FormFocus::Upload.is_files());
    }

    #[test]
    fn overwrite_and_skip_are_exclusive() {
        let mut form = UploadForm::new(1, "demo".into(), None);
        form.focus = FormFocus::Overwrite;
        form.activate();
        assert!(form.overwrite);
        assert!(!form.skip_existing);
        form.focus = FormFocus::SkipExisting;
        form.activate();
        assert!(form.skip_existing);
        assert!(!form.overwrite);
        form.focus = FormFocus::Files;
        form.activate();
        assert!(form.selected.is_empty());
    }

    #[test]
    fn parse_metadata_key_value() {
        assert_eq!(parse_meta("car=17").unwrap(), ("car".into(), json!(17)));
        assert_eq!(
            parse_meta("name=qualifying").unwrap(),
            ("name".into(), json!("qualifying"))
        );
        assert!(parse_meta("nocolon").is_err());
        assert!(parse_meta("=value").is_err());
    }

    #[test]
    fn selected_summary_names_files_and_folders() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("a.csv");
        fs::write(&file, "x").unwrap();
        let mut selected = HashSet::new();
        selected.insert(file);
        assert_eq!(selected_summary(&selected), "1 file");
        selected.insert(tmp.path().to_path_buf());
        let summary = selected_summary(&selected);
        assert!(summary.contains("1 file"));
        assert!(summary.contains("1 folder"));
        selected.clear();
        assert_eq!(selected_summary(&selected), "");
    }
}
