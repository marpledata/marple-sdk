use super::picker::FilePicker;
use super::{App, BrowseLevel, Focus, PAGE_SIZE, Pane};
use crate::table::visible_span;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use marple_db::{Dataset, ProgressReporter};
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};

#[derive(Default)]
pub(super) struct DownloadState {
    pub picker: Option<FilePicker>,
    plan: Option<DatasetTargets>,
    stream_name: String,
    pending_stream: Option<(i64, PathBuf)>,
    queue: VecDeque<QueuedDownload>,
    running: Option<RunningDownload>,
}

#[derive(Clone, Debug)]
pub(super) enum DatasetTargets {
    Stream { id: i64, count: Option<u64> },
    Datasets(Vec<Dataset>),
}

struct QueuedDownload {
    dataset: Dataset,
    dest: PathBuf,
}

struct RunningDownload {
    dataset_id: i64,
    bytes: Arc<AtomicU64>,
    total: u64,
    rx: Receiver<Result<(), String>>,
}

impl DownloadState {
    pub(super) fn clear(&mut self) {
        self.picker = None;
        self.plan = None;
        self.pending_stream = None;
        self.queue.clear();
    }

    pub(super) fn needs_tick(&self) -> bool {
        self.running.is_some() || !self.queue.is_empty() || self.pending_stream.is_some()
    }

    pub(super) fn is_active(&self, dataset_id: i64) -> bool {
        self.running
            .as_ref()
            .is_some_and(|running| running.dataset_id == dataset_id)
            || self
                .queue
                .iter()
                .any(|queued| queued.dataset.id == dataset_id)
    }

    pub(super) fn byte_ratio(&self, dataset_id: i64) -> Option<f64> {
        if let Some(running) = &self.running
            && running.dataset_id == dataset_id
        {
            if running.total == 0 {
                return Some(1.0);
            }
            return Some(
                (running.bytes.load(Ordering::Relaxed) as f64 / running.total as f64)
                    .clamp(0.0, 1.0),
            );
        }
        self.queue
            .iter()
            .any(|queued| queued.dataset.id == dataset_id)
            .then_some(0.0)
    }

    pub(super) fn title(&self) -> String {
        format!("download  /{}  ({})", self.stream_name, self.count_label())
    }

    pub(super) fn footer(&self) -> String {
        format!("enter download {} here", self.count_label())
    }

    fn count_label(&self) -> String {
        match &self.plan {
            Some(DatasetTargets::Datasets(datasets)) => dataset_count_label(datasets.len()),
            Some(DatasetTargets::Stream {
                count: Some(count), ..
            }) => dataset_count_label(*count as usize),
            _ => "all datasets".to_string(),
        }
    }
}

pub(super) fn dataset_count_label(n: usize) -> String {
    format!("{n} dataset{}", if n == 1 { "" } else { "s" })
}

impl App {
    pub(super) fn is_dataset_checked(&self, dataset_id: i64) -> bool {
        self.selected_datasets.contains(&dataset_id)
    }

    pub(super) fn dataset_table_focused(&self) -> bool {
        self.browse_level == BrowseLevel::Streams && self.focus == Focus::Table
    }

    pub(super) fn toggle_dataset_selection(&mut self) {
        if !self.dataset_table_focused() {
            return;
        }
        let Some(id) = self.selected_dataset().map(|dataset| dataset.id) else {
            return;
        };
        self.selection_anchor = self
            .loaded_datasets
            .as_ref()
            .and_then(|loaded| loaded.selected_index());
        if !self.selected_datasets.remove(&id) {
            self.selected_datasets.insert(id);
        }
    }

    pub(super) fn select_dataset_range(&mut self) {
        if !self.dataset_table_focused() {
            return;
        }
        let Some(current) = self
            .loaded_datasets
            .as_ref()
            .and_then(|loaded| loaded.selected_index())
        else {
            return;
        };
        let visible = self.dataset_indices(true);
        for index in visible_span(&visible, self.selection_anchor, current) {
            if let Some(id) = self.datasets().get(index).map(|dataset| dataset.id) {
                self.selected_datasets.insert(id);
            }
        }
        if self.selection_anchor.is_none() {
            self.selection_anchor = Some(current);
        }
    }

    pub(super) fn select_all_datasets(&mut self) {
        if !self.dataset_table_focused() {
            return;
        }
        let visible = self.dataset_indices(true);
        let ids: Vec<i64> = (0..visible.len())
            .filter_map(|pos| visible.get(pos))
            .map(|index| self.datasets()[index].id)
            .collect();
        if ids.is_empty() {
            return;
        }
        let all_checked = ids.iter().all(|id| self.selected_datasets.contains(id));
        if all_checked {
            for id in ids {
                self.selected_datasets.remove(&id);
            }
        } else {
            self.selected_datasets.extend(ids);
        }
    }

    pub(super) fn open_download(&mut self) {
        if self.download.picker.is_some() || self.upload.form.is_some() {
            return;
        }
        if !self.connected {
            self.prompt_for_env();
            return;
        }
        let stream_name = self
            .selected_stream()
            .or_else(|| self.loaded_stream())
            .map(|stream| stream.name.clone())
            .unwrap_or_default();
        let stream_id = self.selected_stream().map(|stream| stream.id);
        let stream_count = if stream_id.is_some_and(|id| self.loaded_stream_id() == Some(id)) {
            Some(self.datasets().len() as u64)
        } else {
            self.selected_stream().and_then(|stream| stream.n_datasets)
        };
        let plan = match dataset_targets(
            self.focused_pane(),
            stream_id,
            stream_count,
            &self.selected_datasets,
            self.datasets(),
            self.selected_dataset(),
        ) {
            Ok(plan) => plan,
            Err(error) => {
                self.status = error;
                return;
            }
        };
        self.status.clear();
        self.download.stream_name = stream_name;
        self.download.plan = Some(plan);
        self.download.picker = Some(FilePicker::open(self.upload_dir.as_deref(), &[]));
    }

    pub(super) fn download_typing(&self) -> bool {
        self.download
            .picker
            .as_ref()
            .is_some_and(|picker| picker.editing)
    }

    pub(super) fn on_download_tick(&mut self) {
        self.apply_download_events();
        self.start_next_download();
    }

    pub(super) fn on_datasets_loaded(&mut self, stream_id: i64) {
        let Some((id, dest)) = self.download.pending_stream.take() else {
            return;
        };
        if id != stream_id {
            self.download.pending_stream = Some((id, dest));
            return;
        }
        let datasets = self.datasets().to_vec();
        self.queue_datasets(datasets, dest);
    }

    pub(super) fn handle_download_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.close_download();
                return;
            }
            KeyCode::Enter => {
                let dest = self.download.picker.as_mut().and_then(take_download_dest);
                if let Some(dest) = dest {
                    self.confirm_download(dest);
                }
                return;
            }
            _ => {}
        }
        let Some(picker) = self.download.picker.as_mut() else {
            return;
        };
        match key.code {
            KeyCode::Char('h') | KeyCode::Left | KeyCode::Backspace => picker.go_parent(),
            KeyCode::Char('l') | KeyCode::Right
                if picker.selected_entry().is_some_and(|entry| entry.is_dir) =>
            {
                picker.enter_selected();
            }
            KeyCode::Char('/') => picker.start_editing(),
            _ => {}
        }
    }

    pub(super) fn handle_download_input(&mut self, key: KeyEvent) {
        let mut status = None;
        let mut dest = None;
        {
            let Some(picker) = self.download.picker.as_mut() else {
                return;
            };
            match key.code {
                KeyCode::Enter => match picker.submit_typed(true) {
                    Ok(Some(path)) => match as_dir(path) {
                        Ok(path) => dest = Some(path),
                        Err(error) => status = Some(error),
                    },
                    Ok(None) => {}
                    Err(error) => status = Some(error),
                },
                KeyCode::Esc => picker.cancel_editing(),
                KeyCode::Backspace => picker.backspace(),
                KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                    picker.push_char(ch);
                }
                _ => {}
            }
        }
        if let Some(error) = status {
            self.status = error;
        }
        if let Some(dest) = dest {
            self.confirm_download(dest);
        }
    }

    pub(super) fn apply_download_motion(&mut self, motion: super::Motion) {
        let Some(picker) = self.download.picker.as_mut() else {
            return;
        };
        let last = picker.len().saturating_sub(1);
        match motion {
            super::Motion::Delta(delta) => picker.move_sel(delta),
            super::Motion::Page(pages) => picker.move_sel(pages * PAGE_SIZE),
            super::Motion::First => picker.goto(0),
            super::Motion::Last => picker.goto(last),
            super::Motion::Goto(index) => picker.goto(index),
        }
    }

    fn close_download(&mut self) {
        if let Some(picker) = &self.download.picker {
            self.upload_dir = Some(picker.dir.clone());
        }
        self.download.picker = None;
        self.download.plan = None;
        self.persist_settings();
    }

    fn confirm_download(&mut self, dest: PathBuf) {
        if let Err(error) = as_dir(dest.clone()) {
            self.status = error;
            return;
        }
        self.upload_dir = Some(dest.clone());
        let plan = self.download.plan.take();
        self.download.picker = None;
        self.persist_settings();
        match plan {
            Some(DatasetTargets::Stream { id, .. }) => self.queue_stream_download(id, dest),
            Some(DatasetTargets::Datasets(datasets)) => {
                if let Some(stream_id) = datasets.first().map(|dataset| dataset.datastream_id) {
                    self.show_dataset_table(stream_id);
                }
                self.queue_datasets(datasets, dest);
            }
            None => self.status = "nothing to download".to_string(),
        }
    }

    fn queue_stream_download(&mut self, stream_id: i64, dest: PathBuf) {
        if self.loaded_stream_id() == Some(stream_id) {
            let datasets = self.datasets().to_vec();
            self.show_dataset_table(stream_id);
            self.queue_datasets(datasets, dest);
            return;
        }
        self.download.pending_stream = Some((stream_id, dest));
        self.show_dataset_table(stream_id);
        self.request_datasets(stream_id);
    }

    pub(super) fn show_dataset_table(&mut self, stream_id: i64) {
        if self.selected_stream().map(|stream| stream.id) != Some(stream_id)
            && let Some(index) = self
                .streams
                .iter()
                .position(|stream| stream.id == stream_id)
        {
            self.stream_state.select(Some(index));
        }
        if matches!(self.browse_level, BrowseLevel::Root | BrowseLevel::Datasets) {
            self.browse_level = BrowseLevel::Streams;
        }
        self.focus = Focus::Table;
    }

    fn queue_datasets(&mut self, datasets: Vec<Dataset>, dest: PathBuf) {
        let mut queued = 0usize;
        let mut skipped = 0usize;
        for dataset in datasets {
            if dataset.backup_size.is_none() {
                skipped += 1;
                continue;
            }
            self.download.queue.push_back(QueuedDownload {
                dataset,
                dest: dest.clone(),
            });
            queued += 1;
        }
        if queued == 0 {
            self.status = if skipped > 0 {
                "no original files to download".to_string()
            } else {
                "no datasets to download".to_string()
            };
            return;
        }
        self.status = if skipped > 0 {
            format!("skipped {skipped} (no backup)")
        } else {
            String::new()
        };
        self.start_next_download();
    }

    fn start_next_download(&mut self) {
        if self.download.running.is_some() {
            return;
        }
        let Some(queued) = self.download.queue.pop_front() else {
            return;
        };
        let total = queued.dataset.backup_size.unwrap_or(0);
        let bytes = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::channel();
        let db = self.db.clone();
        let dataset = queued.dataset.clone();
        let dest = queued.dest;
        let progress = Arc::new(AtomicProgress(Arc::clone(&bytes)));
        tokio::spawn(async move {
            let result = db
                .download_original_with_progress(&dataset, &dest, progress.as_ref())
                .await
                .map(|_| ())
                .map_err(|error| error.to_string());
            let _ = tx.send(result);
        });
        self.download.running = Some(RunningDownload {
            dataset_id: queued.dataset.id,
            bytes,
            total,
            rx,
        });
    }

    fn apply_download_events(&mut self) {
        let result = {
            let Some(running) = self.download.running.as_mut() else {
                return;
            };
            match running.rx.try_recv() {
                Ok(result) => result,
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    self.status = "download task ended unexpectedly".to_string();
                    self.download.running = None;
                    return;
                }
            }
        };
        self.download.running = None;
        if let Err(error) = result {
            self.status = error;
        }
    }
}

pub(super) fn dataset_targets(
    pane: Pane,
    stream_id: Option<i64>,
    stream_count: Option<u64>,
    checked: &HashSet<i64>,
    datasets: &[Dataset],
    focused: Option<&Dataset>,
) -> Result<DatasetTargets, String> {
    match pane {
        Pane::Streams => {
            let stream_id = stream_id.ok_or_else(|| "select a stream first".to_string())?;
            Ok(DatasetTargets::Stream {
                id: stream_id,
                count: stream_count,
            })
        }
        Pane::Datasets | Pane::Signals => {
            if !checked.is_empty() {
                let selected: Vec<Dataset> = datasets
                    .iter()
                    .filter(|dataset| checked.contains(&dataset.id))
                    .cloned()
                    .collect();
                if selected.is_empty() {
                    return Err("select datasets first".to_string());
                }
                return Ok(DatasetTargets::Datasets(selected));
            }
            let dataset = focused.ok_or_else(|| "select a dataset first".to_string())?;
            Ok(DatasetTargets::Datasets(vec![dataset.clone()]))
        }
    }
}

fn take_download_dest(picker: &mut FilePicker) -> Option<PathBuf> {
    if picker
        .selected_entry()
        .is_some_and(|entry| entry.name == "..")
    {
        picker.enter_selected();
        return None;
    }
    Some(destination_from(picker))
}

fn destination_from(picker: &FilePicker) -> PathBuf {
    match picker.selected_entry() {
        Some(entry) if entry.is_dir && entry.name != ".." => entry.path.clone(),
        _ => picker.dir.clone(),
    }
}

fn as_dir(path: PathBuf) -> Result<PathBuf, String> {
    if path.is_dir() {
        Ok(path)
    } else if path.is_file() {
        Err("choose a folder".to_string())
    } else {
        Err(format!("{} is not a folder", path.display()))
    }
}

struct AtomicProgress(Arc<AtomicU64>);

impl ProgressReporter for AtomicProgress {
    fn set_position(&self, position: u64) {
        self.0.store(position, Ordering::Relaxed);
    }

    fn finish(&self) {}
}

#[cfg(test)]
mod tests {
    use super::{
        DatasetTargets, DownloadState, FilePicker, Pane, as_dir, dataset_count_label,
        dataset_targets, destination_from, take_download_dest,
    };
    use crate::table::{Visible, visible_span};
    use marple_db::Dataset;
    use serde_json::json;
    use std::collections::HashSet;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    fn dataset(id: i64, path: &str, backup: Option<u64>) -> Dataset {
        let mut value = json!({
            "id": id,
            "datastream_id": 7,
            "path": path,
            "import_status": "FINISHED"
        });
        if let Some(size) = backup {
            value["backup_size"] = json!(size);
        }
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn stream_pane_downloads_the_whole_stream() {
        let plan =
            dataset_targets(Pane::Streams, Some(4), Some(12), &HashSet::new(), &[], None).unwrap();
        assert!(matches!(
            plan,
            DatasetTargets::Stream {
                id: 4,
                count: Some(12)
            }
        ));
        assert!(dataset_targets(Pane::Streams, None, None, &HashSet::new(), &[], None).is_err());
        let mut state = DownloadState {
            stream_name: "Runs".into(),
            plan: Some(DatasetTargets::Stream {
                id: 4,
                count: Some(12),
            }),
            ..DownloadState::default()
        };
        assert_eq!(state.title(), "download  /Runs  (12 datasets)");
        assert_eq!(state.footer(), "enter download 12 datasets here");
        state.plan = Some(DatasetTargets::Datasets(vec![dataset(1, "a.csv", Some(1))]));
        assert_eq!(state.title(), "download  /Runs  (1 dataset)");
        assert_eq!(dataset_count_label(1), "1 dataset");
        assert_eq!(dataset_count_label(3), "3 datasets");
    }

    #[test]
    fn dataset_pane_uses_checks_then_focused_row() {
        let rows = vec![
            dataset(1, "a.csv", Some(10)),
            dataset(2, "b.csv", Some(20)),
            dataset(3, "c.csv", None),
        ];
        let mut checked = HashSet::new();
        checked.insert(2);
        checked.insert(3);
        let DatasetTargets::Datasets(selected) = dataset_targets(
            Pane::Datasets,
            Some(7),
            None,
            &checked,
            &rows,
            Some(&rows[0]),
        )
        .unwrap() else {
            panic!("expected datasets");
        };
        assert_eq!(
            selected.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![2, 3]
        );

        let DatasetTargets::Datasets(focused) = dataset_targets(
            Pane::Datasets,
            Some(7),
            None,
            &HashSet::new(),
            &rows,
            Some(&rows[0]),
        )
        .unwrap() else {
            panic!("expected focused");
        };
        assert_eq!(focused[0].id, 1);
        assert!(
            dataset_targets(Pane::Datasets, Some(7), None, &HashSet::new(), &rows, None).is_err()
        );
    }

    #[test]
    fn range_select_covers_visible_rows_between_anchor_and_cursor() {
        let rows = vec![
            dataset(1, "a.csv", None),
            dataset(2, "b.csv", None),
            dataset(3, "c.csv", None),
            dataset(4, "d.csv", None),
        ];
        let visible = Visible::filtered(4, vec![0, 2, 3]);
        let mut checked = HashSet::new();
        for index in visible_span(&visible, Some(0), 3) {
            checked.insert(rows[index].id);
        }
        let mut ids: Vec<_> = checked.into_iter().collect();
        ids.sort();
        assert_eq!(ids, vec![1, 3, 4]);
    }

    #[test]
    fn overlay_tracks_queued_and_running_ids() {
        let mut state = DownloadState::default();
        assert!(!state.is_active(1));
        assert!(state.byte_ratio(1).is_none());
        state.queue.push_back(super::QueuedDownload {
            dataset: dataset(1, "a.csv", Some(100)),
            dest: PathBuf::from("."),
        });
        assert!(state.is_active(1));
        assert_eq!(state.byte_ratio(1), Some(0.0));
        state.running = Some(super::RunningDownload {
            dataset_id: 1,
            bytes: Arc::new(AtomicU64::new(40)),
            total: 100,
            rx: std::sync::mpsc::channel().1,
        });
        state.queue.clear();
        assert!(state.is_active(1));
        assert!((state.byte_ratio(1).unwrap() - 0.4).abs() < f64::EPSILON);
        assert!(state.needs_tick());
        state.clear();
        assert!(state.queue.is_empty());
        assert!(state.running.is_some());
        assert!(state.needs_tick());
    }

    #[test]
    fn destination_is_highlighted_folder_or_current_dir() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("out")).unwrap();
        fs::write(tmp.path().join("notes.txt"), "x").unwrap();
        let mut picker = FilePicker::open(Some(tmp.path()), &[]);
        picker.goto(
            picker
                .entries
                .iter()
                .position(|entry| entry.name == "out")
                .unwrap(),
        );
        assert_eq!(
            destination_from(&picker),
            fs::canonicalize(tmp.path().join("out")).unwrap()
        );
        picker.goto(
            picker
                .entries
                .iter()
                .position(|entry| entry.name == "..")
                .unwrap(),
        );
        assert_eq!(
            destination_from(&picker),
            fs::canonicalize(tmp.path()).unwrap()
        );
        let parent = fs::canonicalize(tmp.path().parent().unwrap()).unwrap();
        assert!(take_download_dest(&mut picker).is_none());
        assert_eq!(picker.dir, parent);

        picker = FilePicker::open(Some(tmp.path()), &[]);
        picker.goto(
            picker
                .entries
                .iter()
                .position(|entry| entry.name == "out")
                .unwrap(),
        );
        assert_eq!(
            take_download_dest(&mut picker),
            Some(fs::canonicalize(tmp.path().join("out")).unwrap())
        );
        assert!(as_dir(tmp.path().join("notes.txt")).is_err());
        assert_eq!(as_dir(tmp.path().to_path_buf()).unwrap(), tmp.path());
    }
}
