mod batch;
mod debug;
mod download;
mod draw;
mod format;
mod input;
mod picker;
mod progress;
mod selection;
mod session;
pub(crate) mod style;
mod upload;

use crate::connect;
use crate::table::{TableSearch, Visible, goto_visible, snap_visible, step_visible};
use anyhow::Result;
use batch::BatchState;
use crossterm::event::{Event, EventStream, KeyEventKind};
use debug::DebugState;
use download::DownloadState;
use draw::draw;
use format::{
    ImportMix, dataset_info, dataset_matches, import_mix_of, signal_info, signal_matches,
    stream_info, stream_matches,
};
use futures_util::StreamExt;
use input::{MotionState, handle_key};
use marple_db::{CurrentWorkspace, Dataset, MarpleDB, Signal, Stream};
use picker::FilePicker;
use ratatui::text::Line;
use ratatui::widgets::TableState;
use session::{
    BrowseSettings, RecentEnv, apply_env_file, env_label, load_settings, local_dotenv,
    remember_recent, save_settings,
};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use upload::UploadState;

pub(super) const AUTO_LOAD_LIMIT: u64 = 100;
pub(super) const PAGE_SIZE: i32 = 10;
const NOT_CONNECTED: &str = "not connected — pick an env file (w)";
const SPINNER: [&str; 3] = [".", "..", "..."];
const SPINNER_TICK: std::time::Duration = std::time::Duration::from_millis(200);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Motion {
    Delta(i32),
    Page(i32),
    First,
    Last,
    Goto(usize),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BrowseLevel {
    Root,
    Streams,
    Datasets,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Focus {
    List,
    Table,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum DatasetView {
    #[default]
    Info,
    Debug,
    Signals,
}

impl DatasetView {
    const ALL: [Self; 3] = [Self::Info, Self::Debug, Self::Signals];

    fn next(self) -> Self {
        match self {
            Self::Info => Self::Debug,
            Self::Debug => Self::Signals,
            Self::Signals => Self::Info,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Signals => "signals",
        }
    }
}

/// Which column the cursor is in. Derived from `(BrowseLevel, Focus)`; not stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Pane {
    Streams,
    Datasets,
    Signals,
}

fn pane_at(level: BrowseLevel, focus: Focus) -> Pane {
    match (level, focus) {
        (BrowseLevel::Root, _) | (BrowseLevel::Streams, Focus::List) => Pane::Streams,
        (BrowseLevel::Streams, Focus::Table) | (BrowseLevel::Datasets, Focus::List) => {
            Pane::Datasets
        }
        (BrowseLevel::Datasets, Focus::Table) => Pane::Signals,
    }
}

fn cycle_focus(level: BrowseLevel, focus: Focus) -> Focus {
    if level == BrowseLevel::Root {
        Focus::Table
    } else {
        match focus {
            Focus::List => Focus::Table,
            Focus::Table => Focus::List,
        }
    }
}

fn step_back(level: BrowseLevel, focus: Focus) -> (BrowseLevel, Focus) {
    if focus == Focus::Table && level != BrowseLevel::Root {
        return (level, Focus::List);
    }
    match level {
        BrowseLevel::Datasets => (BrowseLevel::Streams, Focus::Table),
        _ => (BrowseLevel::Root, Focus::Table),
    }
}

#[derive(Clone, Copy)]
enum PendingLoad {
    Streams,
    Datasets(i64),
    Signals { stream_id: i64, dataset_id: i64 },
}

impl PendingLoad {
    fn same_target(self, other: Self) -> bool {
        match (self, other) {
            (Self::Streams, Self::Streams) => true,
            (Self::Datasets(left), Self::Datasets(right)) => left == right,
            (
                Self::Signals {
                    dataset_id: left, ..
                },
                Self::Signals {
                    dataset_id: right, ..
                },
            ) => left == right,
            _ => false,
        }
    }

    fn loaded_in(self, app: &App) -> bool {
        match self {
            Self::Streams => false,
            Self::Datasets(id) => app.loaded_stream_id() == Some(id),
            Self::Signals { dataset_id, .. } => app.signals_dataset_id() == Some(dataset_id),
        }
    }
}

enum Message {
    Input(Event),
    Loaded(u64, PendingLoad, Box<Result<LoadResult, String>>),
    DebugLoaded(u64, i64, Result<Vec<String>, String>),
    Upload(Box<upload::UploadEvent>),
    Statuses(u64, Box<upload::StatusPollResult>),
    Download(Result<(), String>),
    Batch(batch::BatchEvent),
    Tick,
}

enum LoadResult {
    Streams {
        streams: Vec<Stream>,
        workspace: Option<Box<CurrentWorkspace>>,
    },
    Datasets(i64, Vec<Dataset>),
    Signals(i64, Vec<Signal>),
}

struct Loaded<T> {
    parent_id: i64,
    rows: Vec<T>,
    state: TableState,
}

impl<T> Loaded<T> {
    fn new(parent_id: i64, rows: Vec<T>) -> Self {
        let mut state = TableState::default();
        state.select((!rows.is_empty()).then_some(0));
        Self {
            parent_id,
            rows,
            state,
        }
    }

    fn selected(&self) -> Option<&T> {
        self.state.selected().and_then(|index| self.rows.get(index))
    }

    fn selected_index(&self) -> Option<usize> {
        self.state.selected()
    }
}

pub(super) struct App {
    db: MarpleDB,
    url: String,
    env_file: Option<PathBuf>,
    recents: Vec<RecentEnv>,
    env_picker: Option<FilePicker>,
    streams: Vec<Stream>,
    stream_state: TableState,
    loaded_datasets: Option<Loaded<Dataset>>,
    loaded_signals: Option<Loaded<Signal>>,
    pending: Option<PendingLoad>,
    load_gen: u64,
    events: UnboundedSender<Message>,
    browse_level: BrowseLevel,
    focus: Focus,
    status: String,
    connected: bool,
    workspace: Option<CurrentWorkspace>,
    info_expanded: bool,
    info_scroll: u16,
    info_view: u16,
    dataset_view: DatasetView,
    debug: DebugState,
    motion: MotionState,
    load_tick: u8,
    search: TableSearch,
    upload: UploadState,
    download: DownloadState,
    batch: BatchState,
    selected_datasets: HashSet<i64>,
    selection_anchor: Option<usize>,
    dataset_mix: Option<ImportMix>,
    upload_dir: Option<PathBuf>,
}

pub async fn run(db: MarpleDB, url: String, env_file: Option<PathBuf>) -> Result<()> {
    let mut settings = load_settings();
    let env_file = match env_file {
        Some(path) => {
            settings.env_file = Some(path.clone());
            let _ = save_settings(&settings);
            Some(path)
        }
        None => settings
            .env_file
            .clone()
            .filter(|path| path.is_file())
            .or_else(local_dotenv),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut app = App::new(db, url, env_file, settings.recents.clone(), tx);
    app.upload_dir = settings.upload_dir.filter(|path| path.is_dir());
    if let Some(path) = app.env_file.clone()
        && let Err(error) = app.connect_from_path(&path)
    {
        app.status = format!("not connected — {error}");
    }

    let mut terminal = ratatui::init();
    app.request_streams();
    let result = event_loop(&mut terminal, &mut app, rx).await;
    app.persist_settings();
    ratatui::restore();
    result
}

async fn event_loop(
    terminal: &mut ratatui::DefaultTerminal,
    app: &mut App,
    mut events: UnboundedReceiver<Message>,
) -> Result<()> {
    let mut input = EventStream::new();
    loop {
        let mut info_view = app.info_view;
        terminal.draw(|frame| {
            info_view = draw(frame, app);
        })?;
        app.info_view = info_view;
        tokio::select! {
            biased;
            Some(message) = events.recv() => {
                if apply_message(app, message) {
                    break;
                }
            }
            event = input.next() => {
                match event {
                    Some(Ok(event)) => {
                        if apply_message(app, Message::Input(event)) {
                            break;
                        }
                    }
                    Some(Err(error)) => app.status = error.to_string(),
                    None => break,
                }
            }
            _ = tokio::time::sleep(SPINNER_TICK), if app.needs_tick() => {
                apply_message(app, Message::Tick);
            }
        }
    }
    Ok(())
}

fn apply_message(app: &mut App, message: Message) -> bool {
    match message {
        Message::Input(Event::Key(key))
            if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
        {
            handle_key(app, key)
        }
        Message::Input(_) => false,
        Message::Loaded(load_gen, pending, result) => {
            app.apply_load_result(load_gen, pending, *result);
            false
        }
        Message::DebugLoaded(load_gen, dataset_id, result) => {
            app.apply_debug_result(load_gen, dataset_id, result);
            false
        }
        Message::Upload(event) => {
            app.apply_upload_event(*event);
            false
        }
        Message::Statuses(poll_gen, result) => {
            app.apply_status_poll(poll_gen, *result);
            false
        }
        Message::Download(result) => {
            app.apply_download_result(result);
            false
        }
        Message::Batch(event) => {
            app.apply_batch_event(event);
            false
        }
        Message::Tick => {
            app.load_tick = app.load_tick.wrapping_add(1);
            app.on_upload_tick();
            app.on_download_tick();
            app.on_batch_tick();
            false
        }
    }
}

async fn fetch(db: MarpleDB, pending: PendingLoad) -> Result<LoadResult, String> {
    match pending {
        PendingLoad::Streams => {
            let streams = db.get_streams().await.map_err(|error| error.to_string())?;
            let workspace = db.get_current_workspace().await.ok().map(Box::new);
            Ok(LoadResult::Streams { streams, workspace })
        }
        PendingLoad::Datasets(stream_id) => db
            .get_datasets(stream_id)
            .await
            .map(|datasets| LoadResult::Datasets(stream_id, datasets))
            .map_err(|error| error.to_string()),
        PendingLoad::Signals {
            stream_id,
            dataset_id,
        } => db
            .get_signals(stream_id, dataset_id)
            .await
            .map(|signals| LoadResult::Signals(dataset_id, signals))
            .map_err(|error| error.to_string()),
    }
}

impl App {
    fn new(
        db: MarpleDB,
        url: String,
        env_file: Option<PathBuf>,
        recents: Vec<RecentEnv>,
        events: UnboundedSender<Message>,
    ) -> Self {
        Self {
            db,
            url,
            env_file,
            recents,
            env_picker: None,
            streams: Vec::new(),
            stream_state: TableState::default().with_selected(Some(0)),
            loaded_datasets: None,
            loaded_signals: None,
            pending: None,
            load_gen: 0,
            events,
            browse_level: BrowseLevel::Root,
            focus: Focus::Table,
            status: String::new(),
            connected: false,
            workspace: None,
            info_expanded: false,
            info_scroll: 0,
            info_view: 8,
            dataset_view: DatasetView::Info,
            debug: DebugState::default(),
            motion: MotionState::default(),
            load_tick: 0,
            search: TableSearch::default(),
            upload: UploadState::default(),
            download: DownloadState::default(),
            batch: BatchState::default(),
            selected_datasets: HashSet::new(),
            selection_anchor: None,
            dataset_mix: None,
            upload_dir: None,
        }
    }

    fn connect_from_path(&mut self, path: &Path) -> Result<(), String> {
        let (url, token) = apply_env_file(path).map_err(|error| error.to_string())?;
        let db = connect(&url, &token).map_err(|error| error.to_string())?;
        self.db = db;
        self.url = url;
        self.env_file = Some(path.to_path_buf());
        Ok(())
    }

    fn use_env_file(&mut self, path: PathBuf) {
        match self.connect_from_path(&path) {
            Ok(()) => {
                self.clear_loaded();
                self.browse_level = BrowseLevel::Root;
                self.focus = Focus::Table;
                self.info_expanded = false;
                self.info_scroll = 0;
                self.dataset_view = DatasetView::Info;
                self.debug.invalidate();
                self.persist_settings();
                self.env_picker = None;
                self.request_streams();
            }
            Err(error) => self.status = error,
        }
    }

    fn prompt_for_env(&mut self) {
        if self.status.is_empty() {
            self.status = NOT_CONNECTED.to_string();
        }
        self.open_env();
    }

    fn open_env(&mut self) {
        if self.connected {
            self.status.clear();
        }
        self.env_picker = Some(FilePicker::open(self.env_file.as_deref(), &self.recents));
    }

    fn remember_current_env(&mut self) {
        let Some(path) = self.env_file.clone() else {
            return;
        };
        let workspace = self
            .workspace
            .as_ref()
            .map(|workspace| workspace.name.clone())
            .unwrap_or_else(|| env_label(&path));
        self.recents = remember_recent(std::mem::take(&mut self.recents), path, workspace);
    }

    fn persist_settings(&mut self) {
        let settings = BrowseSettings {
            env_file: self.env_file.clone(),
            recents: self.recents.clone(),
            upload_dir: self.upload_dir.clone(),
        };
        if let Err(error) = save_settings(&settings) {
            self.status = error.to_string();
        }
    }

    fn env_label(&self) -> String {
        self.env_file
            .as_deref()
            .map(env_label)
            .unwrap_or_else(|| "env".to_string())
    }

    fn go_back(&mut self) {
        if self.info_expanded {
            self.info_expanded = false;
            self.info_scroll = 0;
            return;
        }
        let (level, focus) = step_back(self.browse_level, self.focus);
        self.browse_level = level;
        self.focus = focus;
    }

    fn activate(&mut self) {
        match self.focused_pane() {
            Pane::Streams => {
                self.search.clear();
                self.open_stream_table();
            }
            Pane::Datasets => {
                if self.browse_level == BrowseLevel::Streams && !self.table_shows_current_stream() {
                    self.search.clear();
                    self.open_stream_table();
                } else if self.browse_level == BrowseLevel::Streams {
                    self.search.clear();
                    self.open_dataset();
                } else {
                    self.cycle_dataset_view();
                }
            }
            Pane::Signals => self.cycle_dataset_view(),
        }
    }

    fn cycle_dataset_view(&mut self) {
        self.dataset_view = self.dataset_view.next();
        self.focus = Focus::Table;
        self.info_scroll = 0;
        self.debug.scroll = 0;
        self.debug.search.editing = false;
        self.on_dataset_view();
    }

    fn on_dataset_view(&mut self) {
        match self.dataset_view {
            DatasetView::Info => {}
            DatasetView::Debug => self.request_debug(),
            DatasetView::Signals => {
                if let Some(dataset) = self.selected_dataset() {
                    self.request_signals(dataset.datastream_id, dataset.id);
                }
            }
        }
    }

    fn toggle_info(&mut self) {
        if self.browse_level == BrowseLevel::Datasets {
            self.dataset_view = DatasetView::Info;
            self.info_scroll = 0;
            return;
        }
        self.info_expanded = !self.info_expanded;
        self.info_scroll = 0;
    }

    fn view_scrolls(&self) -> bool {
        self.browse_level == BrowseLevel::Datasets
            && self.focus == Focus::Table
            && matches!(self.dataset_view, DatasetView::Info | DatasetView::Debug)
    }

    fn scroll_info(&mut self, delta: i32) {
        if self.info_expanded {
            let lines = self.info_for_inspect().1.len() as u16;
            self.info_scroll = clamp_scroll(self.info_scroll, delta, lines, self.info_view);
            return;
        }
        if self.dataset_view == DatasetView::Debug {
            let lines = self.debug_line_count();
            self.debug.scroll = clamp_scroll(self.debug.scroll, delta, lines, self.info_view);
            return;
        }
        let lines = dataset_info(self.selected_dataset()).1.len() as u16;
        self.info_scroll = clamp_scroll(self.info_scroll, delta, lines, self.info_view);
    }

    fn reset_view_scroll(&mut self) {
        if !self.info_expanded && self.dataset_view == DatasetView::Debug {
            self.debug.scroll = 0;
        } else {
            self.info_scroll = 0;
        }
    }

    fn open_stream_table(&mut self) {
        let Some(id) = self.selected_stream().map(|stream| stream.id) else {
            return;
        };
        self.browse_level = BrowseLevel::Streams;
        self.focus = Focus::Table;
        self.status.clear();
        self.request_datasets(id);
    }

    fn open_dataset(&mut self) {
        let Some((stream_id, dataset_id)) = self
            .selected_dataset()
            .map(|dataset| (dataset.datastream_id, dataset.id))
        else {
            return;
        };
        self.browse_level = BrowseLevel::Datasets;
        self.focus = Focus::Table;
        self.dataset_view = DatasetView::Info;
        self.info_scroll = 0;
        self.debug.scroll = 0;
        self.status.clear();
        self.request_signals(stream_id, dataset_id);
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

    fn focused_pane(&self) -> Pane {
        pane_at(self.browse_level, self.focus)
    }

    fn request_streams(&mut self) {
        self.request_unchecked(PendingLoad::Streams);
    }

    fn maybe_autoload_datasets(&mut self) {
        if let Some(stream) = self.selected_stream()
            && stream
                .n_datasets
                .is_some_and(|count| count < AUTO_LOAD_LIMIT)
        {
            self.request_datasets(stream.id);
        }
    }

    fn maybe_autoload_signals(&mut self) {
        if let Some(dataset) = self.selected_dataset()
            && dataset
                .n_signals
                .is_some_and(|count| count < AUTO_LOAD_LIMIT)
        {
            self.request_signals(dataset.datastream_id, dataset.id);
        }
    }

    fn request_datasets(&mut self, stream_id: i64) {
        self.request(PendingLoad::Datasets(stream_id));
    }

    fn request_signals(&mut self, stream_id: i64, dataset_id: i64) {
        self.request(PendingLoad::Signals {
            stream_id,
            dataset_id,
        });
    }

    fn request(&mut self, load: PendingLoad) {
        if !self.connected {
            self.prompt_for_env();
            return;
        }
        self.request_unchecked(load);
    }

    fn request_unchecked(&mut self, load: PendingLoad) {
        if load.loaded_in(self)
            || self
                .pending
                .is_some_and(|pending| pending.same_target(load))
        {
            return;
        }
        self.pending = Some(load);
        self.load_tick = 0;
        self.load_gen = self.load_gen.wrapping_add(1);
        let load_gen = self.load_gen;
        let db = self.db.clone();
        let tx = self.events.clone();
        tokio::spawn(async move {
            let result = fetch(db, load).await;
            let _ = tx.send(Message::Loaded(load_gen, load, Box::new(result)));
        });
    }

    fn needs_tick(&self) -> bool {
        self.pending.is_some()
            || self.debug.pending
            || self.upload.needs_tick()
            || self.download.needs_tick()
            || self.batch.needs_tick()
            || !self.viewport_inflight().is_empty()
    }

    pub(super) fn loading_dots(&self) -> &'static str {
        SPINNER[(self.load_tick as usize) % SPINNER.len()]
    }

    fn is_loading_streams(&self) -> bool {
        matches!(self.pending, Some(PendingLoad::Streams))
    }

    fn is_loading_datasets(&self) -> bool {
        self.selected_stream().is_some_and(|stream| {
            self.pending
                .is_some_and(|pending| pending.same_target(PendingLoad::Datasets(stream.id)))
        })
    }

    fn is_loading_signals(&self) -> bool {
        self.selected_dataset().is_some_and(|dataset| {
            matches!(
                self.pending,
                Some(PendingLoad::Signals { dataset_id, .. }) if dataset_id == dataset.id
            )
        })
    }

    fn apply_load_result(
        &mut self,
        load_gen: u64,
        load: PendingLoad,
        result: Result<LoadResult, String>,
    ) {
        if load_gen != self.load_gen
            || !self
                .pending
                .is_some_and(|pending| pending.same_target(load))
        {
            return;
        }
        self.pending = None;
        match result {
            Ok(LoadResult::Streams { streams, workspace }) => {
                self.apply_streams(streams, workspace);
            }
            Ok(LoadResult::Datasets(stream_id, datasets)) => {
                self.apply_datasets(stream_id, datasets);
            }
            Ok(LoadResult::Signals(dataset_id, signals)) => {
                self.apply_signals(dataset_id, signals);
            }
            Err(error) => {
                if matches!(load, PendingLoad::Streams) {
                    self.connected = false;
                    self.workspace = None;
                    self.streams.clear();
                    self.clear_loaded();
                    self.status = format!("not connected — {error}");
                    self.prompt_for_env();
                } else {
                    self.status = error;
                }
            }
        }
    }

    fn apply_streams(
        &mut self,
        mut streams: Vec<Stream>,
        workspace: Option<Box<CurrentWorkspace>>,
    ) {
        self.connected = true;
        let selected_id = self.selected_stream().map(|stream| stream.id);
        streams.sort_by_key(|stream| stream.id);
        self.streams = streams;
        self.stream_state.select(
            selected_id
                .and_then(|id| self.streams.iter().position(|stream| stream.id == id))
                .or((!self.streams.is_empty()).then_some(0)),
        );
        self.workspace = workspace.map(|workspace| *workspace);
        self.status.clear();
        self.remember_current_env();
        self.env_picker = None;
        self.persist_settings();
    }

    fn apply_datasets(&mut self, stream_id: i64, mut datasets: Vec<Dataset>) {
        datasets.sort_by_key(|dataset| std::cmp::Reverse(dataset.id));
        self.selected_datasets.clear();
        self.selection_anchor = None;
        self.loaded_datasets = Some(Loaded::new(stream_id, datasets));
        self.loaded_signals = None;
        self.debug.invalidate();
        self.sync_dataset_mix();
        self.on_datasets_loaded(stream_id);
        self.on_batch_datasets_loaded(stream_id);
        self.start_next_upload();
    }

    fn apply_signals(&mut self, dataset_id: i64, mut signals: Vec<Signal>) {
        signals.sort_by(|left, right| left.name.cmp(&right.name));
        self.loaded_signals = Some(Loaded::new(dataset_id, signals));
    }

    fn upsert_dataset(&mut self, dataset: Dataset) {
        let stream_id = dataset.datastream_id;
        match &mut self.loaded_datasets {
            Some(loaded) if loaded.parent_id == stream_id => {
                if let Some(index) = loaded.rows.iter().position(|row| row.id == dataset.id) {
                    loaded.rows[index] = dataset;
                } else {
                    loaded.rows.insert(0, dataset);
                    loaded.state.select(Some(0));
                }
            }
            None => self.loaded_datasets = Some(Loaded::new(stream_id, vec![dataset])),
            Some(_) => return,
        }
        self.sync_dataset_mix();
    }

    fn patch_dataset(&mut self, id: i64, patch: impl FnOnce(&mut Dataset)) {
        if let Some(dataset) = self
            .loaded_datasets
            .as_mut()
            .and_then(|loaded| loaded.rows.iter_mut().find(|dataset| dataset.id == id))
        {
            patch(dataset);
        } else {
            return;
        }
        self.sync_dataset_mix();
    }

    fn sync_dataset_mix(&mut self) {
        self.dataset_mix = self
            .loaded_datasets
            .as_ref()
            .map(|loaded| import_mix_of(&loaded.rows));
    }

    fn clear_loaded(&mut self) {
        self.loaded_datasets = None;
        self.loaded_signals = None;
        self.pending = None;
        self.load_gen = self.load_gen.wrapping_add(1);
        self.selected_datasets.clear();
        self.selection_anchor = None;
        self.dataset_mix = None;
        self.debug.invalidate();
        self.dataset_view = DatasetView::Info;
        self.upload.clear();
        self.download.clear();
        self.batch.clear();
    }

    pub(super) fn datasets(&self) -> &[Dataset] {
        self.loaded_datasets
            .as_ref()
            .map(|loaded| loaded.rows.as_slice())
            .unwrap_or(&[])
    }

    pub(super) fn signals(&self) -> &[Signal] {
        self.loaded_signals
            .as_ref()
            .map(|loaded| loaded.rows.as_slice())
            .unwrap_or(&[])
    }

    pub(super) fn loaded_stream_id(&self) -> Option<i64> {
        self.loaded_datasets.as_ref().map(|loaded| loaded.parent_id)
    }

    pub(super) fn signals_dataset_id(&self) -> Option<i64> {
        self.loaded_signals.as_ref().map(|loaded| loaded.parent_id)
    }

    fn selected_stream(&self) -> Option<&Stream> {
        self.stream_state
            .selected()
            .and_then(|index| self.streams.get(index))
    }

    fn loaded_stream(&self) -> Option<&Stream> {
        self.loaded_stream_id()
            .and_then(|id| self.streams.iter().find(|stream| stream.id == id))
    }

    fn table_shows_current_stream(&self) -> bool {
        self.selected_stream().map(|stream| stream.id) == self.loaded_stream_id()
    }

    fn selected_dataset(&self) -> Option<&Dataset> {
        self.loaded_datasets.as_ref().and_then(Loaded::selected)
    }

    fn selected_signal(&self) -> Option<&Signal> {
        self.loaded_signals.as_ref().and_then(Loaded::selected)
    }

    pub(super) fn stream_indices(&self, filtered: bool) -> Visible {
        self.indices(filtered, &self.streams, stream_matches)
    }

    pub(super) fn dataset_indices(&self, filtered: bool) -> Visible {
        self.indices(filtered, self.datasets(), dataset_matches)
    }

    pub(super) fn signal_indices(&self, filtered: bool) -> Visible {
        self.indices(filtered, self.signals(), signal_matches)
    }

    fn indices<T>(
        &self,
        filtered: bool,
        items: &[T],
        matches: impl Fn(&T, &str) -> bool,
    ) -> Visible {
        let query = self.search.query.trim();
        if filtered && self.search.active() && !query.is_empty() {
            Visible::filtered(
                items.len(),
                (0..items.len())
                    .filter(|&index| matches(&items[index], query))
                    .collect(),
            )
        } else {
            Visible::All(items.len())
        }
    }

    fn focused_visible(&self) -> Visible {
        match self.focused_pane() {
            Pane::Streams => self.stream_indices(true),
            Pane::Datasets => self.dataset_indices(true),
            Pane::Signals => self.signal_indices(true),
        }
    }

    fn snap_search(&mut self) {
        let visible = self.focused_visible();
        self.apply_selection(|state| {
            state.select(snap_visible(&visible, state.selected()));
        });
    }

    fn move_sel(&mut self, delta: i32) {
        let visible = self.focused_visible();
        self.apply_selection(|state| {
            state.select(step_visible(&visible, state.selected(), delta));
        });
    }

    fn goto_sel(&mut self, index: usize) {
        let visible = self.focused_visible();
        self.apply_selection(|state| state.select(goto_visible(&visible, index)));
    }

    fn apply_selection(&mut self, select: impl FnOnce(&mut TableState)) {
        if self.info_expanded {
            self.info_scroll = 0;
        }
        match self.focused_pane() {
            Pane::Streams => {
                select(&mut self.stream_state);
                if self.browse_level == BrowseLevel::Streams && self.focus == Focus::List {
                    self.maybe_autoload_datasets();
                }
            }
            Pane::Datasets => {
                if let Some(loaded) = &mut self.loaded_datasets {
                    select(&mut loaded.state);
                }
                if self.focus == Focus::List {
                    self.info_scroll = 0;
                    self.debug.scroll = 0;
                    self.maybe_autoload_signals();
                    if self.dataset_view == DatasetView::Debug {
                        self.request_debug();
                    }
                }
            }
            Pane::Signals => {
                if let Some(loaded) = &mut self.loaded_signals {
                    select(&mut loaded.state);
                }
            }
        }
    }

    fn breadcrumb_path(&self) -> String {
        match self.browse_level {
            BrowseLevel::Root => "/".to_string(),
            BrowseLevel::Streams => self
                .selected_stream()
                .map(|stream| format!("/{}", stream.name))
                .unwrap_or_else(|| "/".to_string()),
            BrowseLevel::Datasets => {
                let stream = self
                    .loaded_stream()
                    .or_else(|| self.selected_stream())
                    .map(|stream| stream.name.as_str())
                    .unwrap_or("");
                match self.selected_dataset() {
                    Some(dataset) => format!("/{stream}/{}", dataset.path),
                    None => format!("/{stream}"),
                }
            }
        }
    }

    fn loaded_import_mix(&self) -> Option<ImportMix> {
        let id = self.selected_stream()?.id;
        (self.loaded_stream_id() == Some(id))
            .then_some(self.dataset_mix)
            .flatten()
    }

    pub(super) fn dataset_tabs(&self) -> String {
        DatasetView::ALL
            .into_iter()
            .map(|view| {
                if view == self.dataset_view {
                    format!("[{}]", view.label())
                } else {
                    view.label().to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
    }

    fn info_for_inspect(&self) -> (String, Vec<Line<'static>>) {
        match self.focused_pane() {
            Pane::Streams => stream_info(
                self.selected_stream(),
                true,
                self.loaded_import_mix().map(|mix| (mix.finished, mix.live)),
            ),
            Pane::Datasets => dataset_info(self.selected_dataset()),
            Pane::Signals => signal_info(self.selected_signal()),
        }
    }
}

fn clamp_scroll(scroll: u16, delta: i32, lines: u16, view: u16) -> u16 {
    let max = lines.saturating_sub(view.saturating_sub(2).max(1));
    (scroll as i32).saturating_add(delta).clamp(0, max as i32) as u16
}

#[cfg(test)]
mod tests {
    use super::{
        BrowseLevel, DatasetView, Focus, Pane, PendingLoad, clamp_scroll, cycle_focus, pane_at,
        step_back,
    };

    #[test]
    fn dataset_view_cycles_info_debug_signals() {
        assert_eq!(DatasetView::Info.next(), DatasetView::Debug);
        assert_eq!(DatasetView::Debug.next(), DatasetView::Signals);
        assert_eq!(DatasetView::Signals.next(), DatasetView::Info);
    }

    #[test]
    fn pending_streams_match_only_streams() {
        assert!(PendingLoad::Streams.same_target(PendingLoad::Streams));
        assert!(!PendingLoad::Streams.same_target(PendingLoad::Datasets(1)));
        assert!(PendingLoad::Datasets(1).same_target(PendingLoad::Datasets(1)));
        assert!(!PendingLoad::Datasets(1).same_target(PendingLoad::Datasets(2)));
    }

    #[test]
    fn pane_follows_level_and_focus() {
        assert_eq!(pane_at(BrowseLevel::Root, Focus::Table), Pane::Streams);
        assert_eq!(pane_at(BrowseLevel::Streams, Focus::List), Pane::Streams);
        assert_eq!(pane_at(BrowseLevel::Streams, Focus::Table), Pane::Datasets);
        assert_eq!(pane_at(BrowseLevel::Datasets, Focus::List), Pane::Datasets);
        assert_eq!(pane_at(BrowseLevel::Datasets, Focus::Table), Pane::Signals);
    }

    #[test]
    fn back_moves_to_list_then_pops_level() {
        assert_eq!(
            step_back(BrowseLevel::Datasets, Focus::Table),
            (BrowseLevel::Datasets, Focus::List)
        );
        assert_eq!(
            step_back(BrowseLevel::Datasets, Focus::List),
            (BrowseLevel::Streams, Focus::Table)
        );
        assert_eq!(
            step_back(BrowseLevel::Streams, Focus::List),
            (BrowseLevel::Root, Focus::Table)
        );
        assert_eq!(
            step_back(BrowseLevel::Root, Focus::Table),
            (BrowseLevel::Root, Focus::Table)
        );
        assert_eq!(cycle_focus(BrowseLevel::Root, Focus::List), Focus::Table);
        assert_eq!(cycle_focus(BrowseLevel::Streams, Focus::List), Focus::Table);
    }

    #[test]
    fn clamp_scroll_saturates_instead_of_wrapping() {
        assert_eq!(clamp_scroll(0, -10, 20, 8), 0);
        assert_eq!(clamp_scroll(0, i32::MAX, 20, 8), 14);
        assert_eq!(clamp_scroll(5, 1, 20, 8), 6);
    }
}
