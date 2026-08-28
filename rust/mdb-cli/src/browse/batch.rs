use super::download::{DatasetTargets, dataset_targets};
use super::{App, BrowseLevel, Focus};
use crossterm::event::{KeyCode, KeyEvent};
use marple_db::{Dataset, ImportStatus};
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BatchKind {
    Delete,
    Reingest,
}

#[derive(Default)]
pub(super) struct BatchState {
    pub confirm: Option<Vec<Dataset>>,
    pending_stream: Option<(BatchKind, i64)>,
    queue: VecDeque<BatchItem>,
    running: Option<RunningBatch>,
}

struct BatchItem {
    kind: BatchKind,
    dataset: Dataset,
}

struct RunningBatch {
    kind: BatchKind,
    dataset_id: i64,
}

pub(super) enum BatchEvent {
    Deleted(i64),
    Reingested(i64, i64, Option<Box<Dataset>>),
    Failed(String),
}

impl BatchState {
    pub(super) fn clear(&mut self) {
        self.confirm = None;
        self.pending_stream = None;
        self.queue.clear();
    }

    pub(super) fn needs_tick(&self) -> bool {
        self.running.is_some() || !self.queue.is_empty() || self.pending_stream.is_some()
    }

    pub(super) fn confirming(&self) -> bool {
        self.confirm.is_some()
    }

    pub(super) fn is_deleting(&self, dataset_id: i64) -> bool {
        self.running.as_ref().is_some_and(|running| {
            running.kind == BatchKind::Delete && running.dataset_id == dataset_id
        }) || self
            .queue
            .iter()
            .any(|item| item.kind == BatchKind::Delete && item.dataset.id == dataset_id)
    }
}

impl App {
    pub(super) fn on_batch_tick(&mut self) {
        self.start_next_batch();
    }

    pub(super) fn on_batch_datasets_loaded(&mut self, stream_id: i64) {
        let Some((kind, id)) = self.batch.pending_stream.take() else {
            return;
        };
        if id != stream_id {
            self.batch.pending_stream = Some((kind, id));
            return;
        }
        let datasets = self.datasets().to_vec();
        if datasets.is_empty() {
            self.status = "no datasets".to_string();
            return;
        }
        match kind {
            BatchKind::Delete => self.prompt_delete(datasets),
            BatchKind::Reingest => self.queue_batch(BatchKind::Reingest, datasets),
        }
    }

    pub(super) fn handle_confirm_key(&mut self, key: KeyEvent) -> bool {
        if !self.batch.confirming() {
            return false;
        }
        match key.code {
            KeyCode::Enter => self.confirm_delete(),
            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('h') | KeyCode::Left => {
                self.batch.confirm = None;
                self.status.clear();
            }
            _ => {}
        }
        true
    }

    pub(super) fn request_delete(&mut self) {
        if self.batch.confirming() {
            return;
        }
        if !self.connected {
            self.prompt_for_env();
            return;
        }
        match self.batch_datasets() {
            Ok(BatchResolve::Ready(datasets)) => self.prompt_delete(datasets),
            Ok(BatchResolve::Load(stream_id)) => {
                self.batch.pending_stream = Some((BatchKind::Delete, stream_id));
                self.show_dataset_table(stream_id);
                self.status = "loading datasets to delete".to_string();
                self.request_datasets(stream_id);
            }
            Err(error) => self.status = error,
        }
    }

    pub(super) fn request_reingest(&mut self) {
        if self.batch.confirming() {
            return;
        }
        if !self.connected {
            self.prompt_for_env();
            return;
        }
        match self.batch_datasets() {
            Ok(BatchResolve::Ready(datasets)) => self.queue_batch(BatchKind::Reingest, datasets),
            Ok(BatchResolve::Load(stream_id)) => {
                self.batch.pending_stream = Some((BatchKind::Reingest, stream_id));
                self.show_dataset_table(stream_id);
                self.status = "loading datasets to reingest".to_string();
                self.request_datasets(stream_id);
            }
            Err(error) => self.status = error,
        }
    }

    fn prompt_delete(&mut self, datasets: Vec<Dataset>) {
        if datasets.is_empty() {
            self.status = "no datasets to delete".to_string();
            return;
        }
        let n = datasets.len();
        self.status = format!(
            "enter to delete {n} dataset{}, esc cancel",
            if n == 1 { "" } else { "s" }
        );
        self.batch.confirm = Some(datasets);
    }

    fn confirm_delete(&mut self) {
        let Some(datasets) = self.batch.confirm.take() else {
            return;
        };
        self.status.clear();
        self.queue_batch(BatchKind::Delete, datasets);
    }

    fn batch_datasets(&self) -> Result<BatchResolve, String> {
        let stream_count = self.selected_stream().and_then(|stream| stream.n_datasets);
        let loaded = self
            .selected_stream()
            .map(|stream| stream.id)
            .filter(|&id| self.loaded_stream_id() == Some(id))
            .map(|_| self.datasets().len() as u64);
        match dataset_targets(
            self.focused_pane(),
            self.selected_stream().map(|stream| stream.id),
            loaded.or(stream_count),
            &self.selected_datasets,
            self.datasets(),
            self.selected_dataset(),
        )? {
            DatasetTargets::Stream { id, .. } => {
                if self.loaded_stream_id() == Some(id) {
                    let datasets = self.datasets().to_vec();
                    if datasets.is_empty() {
                        return Err("no datasets".to_string());
                    }
                    Ok(BatchResolve::Ready(datasets))
                } else {
                    Ok(BatchResolve::Load(id))
                }
            }
            DatasetTargets::Datasets(datasets) => Ok(BatchResolve::Ready(datasets)),
        }
    }

    fn queue_batch(&mut self, kind: BatchKind, datasets: Vec<Dataset>) {
        if datasets.is_empty() {
            self.status = "no datasets".to_string();
            return;
        }
        if let Some(stream_id) = datasets.first().map(|dataset| dataset.datastream_id) {
            self.show_dataset_table(stream_id);
        }
        for dataset in datasets {
            self.batch.queue.push_back(BatchItem { kind, dataset });
        }
        self.start_next_batch();
    }

    fn start_next_batch(&mut self) {
        if self.batch.running.is_some() {
            return;
        }
        let Some(item) = self.batch.queue.pop_front() else {
            return;
        };
        let tx = self.events.clone();
        let db = self.db.clone();
        let kind = item.kind;
        let stream_id = item.dataset.datastream_id;
        let dataset_id = item.dataset.id;
        tokio::spawn(async move {
            let event = match kind {
                BatchKind::Delete => match db.delete_dataset(stream_id, dataset_id).await {
                    Ok(()) => BatchEvent::Deleted(dataset_id),
                    Err(error) => BatchEvent::Failed(error.to_string()),
                },
                BatchKind::Reingest => match db.reingest_dataset(stream_id, dataset_id).await {
                    Ok(()) => {
                        let dataset = db
                            .get_dataset(stream_id, dataset_id)
                            .await
                            .ok()
                            .map(Box::new);
                        BatchEvent::Reingested(stream_id, dataset_id, dataset)
                    }
                    Err(error) => BatchEvent::Failed(error.to_string()),
                },
            };
            let _ = tx.send(super::Message::Batch(event));
        });
        self.batch.running = Some(RunningBatch { kind, dataset_id });
    }

    pub(super) fn apply_batch_event(&mut self, event: BatchEvent) {
        self.batch.running = None;
        match event {
            BatchEvent::Deleted(id) => self.remove_dataset(id),
            BatchEvent::Reingested(stream_id, dataset_id, dataset) => {
                if let Some(dataset) = dataset {
                    self.upsert_dataset(*dataset);
                } else {
                    self.patch_dataset(dataset_id, |dataset| {
                        dataset.import_status = ImportStatus::Waiting;
                    });
                }
                self.upload.watch_dataset(stream_id, dataset_id);
            }
            BatchEvent::Failed(error) => self.status = error,
        }
        self.start_next_batch();
    }

    fn remove_dataset(&mut self, id: i64) {
        self.selected_datasets.remove(&id);
        if self.signals_dataset_id() == Some(id) {
            self.loaded_signals = None;
            if self.browse_level == BrowseLevel::Datasets {
                self.browse_level = BrowseLevel::Streams;
                self.focus = Focus::Table;
            }
        }
        let Some(loaded) = self.loaded_datasets.as_mut() else {
            return;
        };
        let Some(index) = loaded.rows.iter().position(|dataset| dataset.id == id) else {
            return;
        };
        loaded.rows.remove(index);
        if loaded.rows.is_empty() {
            loaded.state.select(None);
        } else {
            let selected = loaded
                .state
                .selected()
                .unwrap_or(0)
                .min(loaded.rows.len() - 1);
            loaded.state.select(Some(selected));
        }
    }
}

enum BatchResolve {
    Ready(Vec<Dataset>),
    Load(i64),
}

#[cfg(test)]
mod tests {
    use super::{BatchItem, BatchKind, BatchState, RunningBatch};
    use marple_db::Dataset;
    use serde_json::json;

    fn dataset(id: i64) -> Dataset {
        serde_json::from_value(json!({
            "id": id,
            "datastream_id": 7,
            "path": "a.csv",
            "import_status": "FINISHED"
        }))
        .unwrap()
    }

    #[test]
    fn is_deleting_covers_queued_and_running() {
        let mut state = BatchState::default();
        assert!(!state.is_deleting(1));
        state.queue.push_back(BatchItem {
            kind: BatchKind::Delete,
            dataset: dataset(1),
        });
        state.queue.push_back(BatchItem {
            kind: BatchKind::Reingest,
            dataset: dataset(2),
        });
        assert!(state.is_deleting(1));
        assert!(!state.is_deleting(2));
        state.queue.clear();
        state.running = Some(RunningBatch {
            kind: BatchKind::Delete,
            dataset_id: 3,
        });
        assert!(state.is_deleting(3));
        assert!(!state.is_deleting(1));
    }
}
