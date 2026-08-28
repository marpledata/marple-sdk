mod draw;
mod format;
mod picker;
mod session;
pub(crate) mod style;

use crate::connect;
use crate::table::{
    TableSearch, filter_indices, goto_visible, handle_search_key, snap_visible, step_visible,
};
use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use draw::draw;
use format::{
    ImportMix, dataset_info, signal_info, signal_kind, signal_source, stream_info, stream_kind,
};
use marple_db::{CurrentWorkspace, Dataset, ImportStatus, MarpleDB, Signal, Stream};
use picker::FilePicker;
use ratatui::text::Line;
use ratatui::widgets::TableState;
use session::{
    BrowseSettings, RecentEnv, apply_env_file, env_label, load_settings, local_dotenv,
    remember_recent, save_settings,
};
use std::path::PathBuf;
use std::time::Duration;

pub(super) const AUTO_LOAD_LIMIT: u64 = 100;
const PAGE_SIZE: i32 = 10;
const NOT_CONNECTED: &str = "not connected — pick an env file (v)";
const SPINNER: [&str; 3] = [".", "..", "..."];
const SPINNER_TICK: std::time::Duration = std::time::Duration::from_millis(200);

fn is_cheap(count: Option<u64>) -> bool {
    count.is_some_and(|count| count < AUTO_LOAD_LIMIT)
}

enum Motion {
    Delta(i32),
    Page(i32),
    First,
    Last,
    Goto(usize),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum BrowseLevel {
    Root,
    Streams,
    Datasets,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Focus {
    List,
    Table,
}

impl Focus {
    fn other(self) -> Self {
        match self {
            Self::List => Self::Table,
            Self::Table => Self::List,
        }
    }
}

pub(super) struct App {
    db: MarpleDB,
    url: String,
    env_file: Option<PathBuf>,
    recents: Vec<RecentEnv>,
    env_picker: Option<FilePicker>,
    streams: Vec<Stream>,
    datasets: Vec<Dataset>,
    signals: Vec<Signal>,
    loaded_stream_id: Option<i64>,
    signals_dataset_id: Option<i64>,
    loading_datasets: Option<i64>,
    loading_signals: Option<(i64, i64)>,
    stream_state: TableState,
    dataset_state: TableState,
    signal_state: TableState,
    browse_level: BrowseLevel,
    focus: Focus,
    status: String,
    connected: bool,
    workspace: Option<CurrentWorkspace>,
    info_expanded: bool,
    info_scroll: u16,
    info_view: u16,
    motion_count: Option<u32>,
    pending_g: bool,
    load_tick: u8,
    search: TableSearch,
}

pub async fn run(mut db: MarpleDB, mut url: String, env_file: Option<PathBuf>) -> Result<()> {
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
    if let Some(path) = &env_file
        && let Ok((next_url, next_token)) = apply_env_file(path)
        && let Ok(next_db) = connect(&next_url, &next_token)
    {
        db = next_db;
        url = next_url;
    }
    let mut app = App::new(db, url, env_file, settings.recents.clone());
    app.refresh_streams().await;
    app.restore_session(&settings).await;
    if app.connected {
        app.remember_current_env();
        app.persist_settings();
    } else {
        app.prompt_for_env();
    }

    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut app).await;
    app.persist_settings();
    ratatui::restore();
    result
}

async fn event_loop(terminal: &mut ratatui::DefaultTerminal, app: &mut App) -> Result<()> {
    loop {
        terminal.draw(|frame| draw(frame, app))?;
        if app.has_pending_load() {
            run_pending_load(terminal, app).await;
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        if handle_key(app, key).await {
            break;
        }
    }
    Ok(())
}

enum LoadResult {
    Datasets(i64, Vec<Dataset>),
    Signals(i64, Vec<Signal>),
}

async fn run_pending_load(terminal: &mut ratatui::DefaultTerminal, app: &mut App) {
    let db = app.db.clone();
    let loading_datasets = app.loading_datasets;
    let loading_signals = app.loading_signals;
    let fetch = async {
        if let Some(stream_id) = loading_datasets {
            return db
                .get_datasets(stream_id)
                .await
                .map(|datasets| LoadResult::Datasets(stream_id, datasets))
                .map_err(|error| error.to_string());
        }
        if let Some((stream_id, dataset_id)) = loading_signals {
            return db
                .get_signals(stream_id, dataset_id)
                .await
                .map(|signals| LoadResult::Signals(dataset_id, signals))
                .map_err(|error| error.to_string());
        }
        Err("nothing to load".to_string())
    };
    tokio::pin!(fetch);
    loop {
        terminal.draw(|frame| draw(frame, app)).ok();
        tokio::select! {
            result = &mut fetch => {
                app.apply_load_result(result);
                return;
            }
            _ = tokio::time::sleep(SPINNER_TICK) => {
                app.load_tick = app.load_tick.wrapping_add(1);
            }
        }
    }
}

async fn handle_key(app: &mut App, mut key: KeyEvent) -> bool {
    loop {
        if matches!(key.code, KeyCode::Esc) && app.has_pending_motion() {
            app.clear_motion();
            return false;
        }
        if app.env_picker.as_ref().is_some_and(|picker| picker.editing) {
            return handle_env_input(app, key).await;
        }
        if app.search.editing {
            handle_search_key(&mut app.search, key);
            app.snap_search();
            return false;
        }
        match read_motion(app, key) {
            MotionRead::Pending => return false,
            MotionRead::Act(motion) => {
                let (motion, leftover) = coalesce_motion(motion);
                apply_motion(app, motion);
                match leftover {
                    Some(next) => {
                        key = next;
                        continue;
                    }
                    None => return false,
                }
            }
            MotionRead::None => {}
        }
        if app.env_picker.is_some() {
            return handle_env_key(app, key).await;
        }
        if matches!(key.code, KeyCode::Char('/')) && !app.info_expanded {
            app.clear_motion();
            app.search.start();
            return false;
        }
        if matches!(key.code, KeyCode::Esc) && app.search.active() {
            app.search.clear();
            return false;
        }
        match key.code {
            KeyCode::Char('q') => return true,
            KeyCode::Tab | KeyCode::BackTab => {
                app.focus = app.cycle_focus();
                app.snap_search();
            }
            KeyCode::Esc | KeyCode::Char('h') | KeyCode::Left => app.go_back(),
            KeyCode::Char('l') | KeyCode::Right | KeyCode::Enter => app.activate(),
            KeyCode::Char('i') => app.toggle_info(),
            KeyCode::Char('v') => app.open_env(),
            _ => {}
        }
        return false;
    }
}

async fn handle_env_key(app: &mut App, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => app.env_picker = None,
        KeyCode::Enter => {
            if let Some(path) = app.env_picker.as_mut().and_then(FilePicker::enter_selected) {
                app.use_env_file(path).await;
            }
        }
        other => {
            let Some(picker) = app.env_picker.as_mut() else {
                return false;
            };
            match other {
                KeyCode::Char('h') | KeyCode::Left | KeyCode::Backspace => picker.go_parent(),
                KeyCode::Char('l') | KeyCode::Right
                    if picker.selected_entry().is_some_and(|entry| entry.is_dir) =>
                {
                    picker.enter_selected();
                }
                KeyCode::Tab | KeyCode::BackTab => picker.cycle_section(),
                KeyCode::Char('/') => picker.start_editing(),
                _ => {}
            }
        }
    }
    false
}

async fn handle_env_input(app: &mut App, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Enter => {
            let result = app
                .env_picker
                .as_mut()
                .map(FilePicker::submit_input)
                .unwrap_or(Err("no picker".to_string()));
            match result {
                Ok(Some(path)) => app.use_env_file(path).await,
                Ok(None) => {}
                Err(error) => app.status = error,
            }
        }
        other => {
            let Some(picker) = app.env_picker.as_mut() else {
                return false;
            };
            match other {
                KeyCode::Esc => picker.cancel_editing(),
                KeyCode::Backspace => picker.backspace(),
                KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                    picker.push_char(ch);
                }
                _ => {}
            }
        }
    }
    false
}

enum MotionRead {
    Pending,
    Act(Motion),
    None,
}

fn read_motion(app: &mut App, key: KeyEvent) -> MotionRead {
    if let KeyCode::Char(c) = key.code
        && c.is_ascii_digit()
        && !key.modifiers.contains(KeyModifiers::SHIFT)
    {
        if c == '0' && app.motion_count.is_none() && !app.pending_g {
            return MotionRead::Act(Motion::First);
        }
        let Some(digit) = c.to_digit(10) else {
            return MotionRead::None;
        };
        app.motion_count = Some(
            app.motion_count
                .unwrap_or(0)
                .saturating_mul(10)
                .saturating_add(digit),
        );
        return MotionRead::Pending;
    }

    let shift = key.modifiers.contains(KeyModifiers::SHIFT);
    let count = app.motion_count.unwrap_or(1) as i32;
    let motion = match key.code {
        KeyCode::Char('g') if !shift => {
            if app.pending_g {
                let n = app.motion_count.take().unwrap_or(1);
                app.pending_g = false;
                return MotionRead::Act(Motion::Goto(n.saturating_sub(1) as usize));
            }
            app.pending_g = true;
            return MotionRead::Pending;
        }
        KeyCode::Char('G') => match app.motion_count.take() {
            Some(n) => Motion::Goto(n.saturating_sub(1) as usize),
            None => Motion::Last,
        },
        KeyCode::Char('J') | KeyCode::PageDown => Motion::Page(count),
        KeyCode::Char('K') | KeyCode::PageUp => Motion::Page(-count),
        KeyCode::Down if shift => Motion::Page(count),
        KeyCode::Up if shift => Motion::Page(-count),
        KeyCode::Char('j') | KeyCode::Down => Motion::Delta(count),
        KeyCode::Char('k') | KeyCode::Up => Motion::Delta(-count),
        _ => {
            app.clear_motion();
            return MotionRead::None;
        }
    };
    app.clear_motion();
    MotionRead::Act(motion)
}

fn arrow_delta(key: KeyEvent) -> Option<i32> {
    if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
        return None;
    }
    match key.code {
        KeyCode::Down => Some(1),
        KeyCode::Up => Some(-1),
        _ => None,
    }
}

fn coalesce_motion(motion: Motion) -> (Motion, Option<KeyEvent>) {
    let Motion::Delta(mut delta) = motion else {
        return (motion, None);
    };
    let leftover = drain_arrow_delta(&mut delta);
    (Motion::Delta(delta), leftover)
}

fn drain_arrow_delta(delta: &mut i32) -> Option<KeyEvent> {
    while event::poll(Duration::ZERO).ok()? {
        match event::read() {
            Ok(Event::Key(key)) => match arrow_delta(key) {
                Some(step) => *delta += step,
                None if key.kind != KeyEventKind::Press => {}
                None => return Some(key),
            },
            Ok(_) => {}
            Err(_) => break,
        }
    }
    None
}

fn apply_motion(app: &mut App, motion: Motion) {
    if app.env_picker.is_some() {
        apply_env_motion(app, motion);
    } else {
        apply_browse_motion(app, motion);
    }
}

fn apply_env_motion(app: &mut App, motion: Motion) {
    let Some(picker) = app.env_picker.as_mut() else {
        return;
    };
    let last = picker.len().saturating_sub(1);
    match motion {
        Motion::Delta(delta) => picker.move_sel(delta),
        Motion::Page(pages) => picker.move_sel(pages * PAGE_SIZE),
        Motion::First => picker.goto(0),
        Motion::Last => picker.goto(last),
        Motion::Goto(index) => picker.goto(index),
    }
}

fn apply_browse_motion(app: &mut App, motion: Motion) {
    match motion {
        Motion::Delta(delta) => app.move_sel(delta),
        Motion::Page(pages) => {
            let delta = pages * PAGE_SIZE;
            if app.info_expanded {
                app.scroll_info(delta);
            } else {
                app.move_sel(delta);
            }
        }
        Motion::First => app.goto_sel(0),
        Motion::Last => app.goto_sel(usize::MAX),
        Motion::Goto(index) => app.goto_sel(index),
    }
}

impl App {
    fn new(db: MarpleDB, url: String, env_file: Option<PathBuf>, recents: Vec<RecentEnv>) -> Self {
        Self {
            db,
            url,
            env_file,
            recents,
            env_picker: None,
            streams: Vec::new(),
            datasets: Vec::new(),
            signals: Vec::new(),
            loaded_stream_id: None,
            signals_dataset_id: None,
            loading_datasets: None,
            loading_signals: None,
            stream_state: TableState::default().with_selected(Some(0)),
            dataset_state: TableState::default(),
            signal_state: TableState::default(),
            browse_level: BrowseLevel::Root,
            focus: Focus::Table,
            status: String::new(),
            connected: false,
            workspace: None,
            info_expanded: false,
            info_scroll: 0,
            info_view: 8,
            motion_count: None,
            pending_g: false,
            load_tick: 0,
            search: TableSearch::default(),
        }
    }

    fn has_pending_motion(&self) -> bool {
        self.motion_count.is_some() || self.pending_g
    }

    fn clear_motion(&mut self) {
        self.motion_count = None;
        self.pending_g = false;
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

    fn go_back(&mut self) {
        if self.info_expanded {
            self.info_expanded = false;
            self.info_scroll = 0;
            return;
        }
        match (self.browse_level, self.focus) {
            (BrowseLevel::Root, _) => {}
            (BrowseLevel::Streams, Focus::Table) => self.focus = Focus::List,
            (BrowseLevel::Streams, Focus::List) => {
                self.browse_level = BrowseLevel::Root;
                self.focus = Focus::Table;
            }
            (BrowseLevel::Datasets, Focus::Table) => {
                self.focus = Focus::List;
            }
            (BrowseLevel::Datasets, Focus::List) => {
                self.browse_level = BrowseLevel::Streams;
                self.focus = Focus::List;
            }
        }
    }

    fn cycle_focus(&self) -> Focus {
        if self.browse_level == BrowseLevel::Root {
            Focus::Table
        } else {
            self.focus.other()
        }
    }

    async fn refresh_streams(&mut self) {
        match self.db.get_streams().await {
            Ok(streams) => {
                self.connected = true;
                let selected_id = self.selected_stream().map(|stream| stream.id);
                self.streams = streams;
                self.streams.sort_by_key(|stream| stream.id);
                self.stream_state.select(
                    selected_id
                        .and_then(|id| self.streams.iter().position(|stream| stream.id == id))
                        .or(if self.streams.is_empty() {
                            None
                        } else {
                            Some(0)
                        }),
                );
                self.refresh_workspace().await;
                self.status.clear();
            }
            Err(error) => {
                self.connected = false;
                self.workspace = None;
                self.streams.clear();
                self.clear_loaded();
                self.status = format!("not connected — {error}");
            }
        }
    }

    async fn refresh_workspace(&mut self) {
        match self.db.get_current_workspace().await {
            Ok(workspace) => self.workspace = Some(workspace),
            Err(_) => self.workspace = None,
        }
    }

    fn activate(&mut self) {
        self.search.clear();
        match (self.browse_level, self.focus) {
            (BrowseLevel::Root, _) | (BrowseLevel::Streams, Focus::List) => {
                self.open_stream_table();
            }
            (BrowseLevel::Streams, Focus::Table) => {
                if self.table_shows_current_stream() {
                    self.open_signals();
                } else {
                    self.open_stream_table();
                }
            }
            (BrowseLevel::Datasets, Focus::List) => self.open_signals(),
            (BrowseLevel::Datasets, Focus::Table) => self.toggle_info(),
        }
    }

    fn toggle_info(&mut self) {
        self.info_expanded = !self.info_expanded;
        self.info_scroll = 0;
    }

    fn scroll_info(&mut self, delta: i32) {
        let lines = self.info_for_inspect().1.len() as u16;
        self.info_scroll = clamp_scroll(self.info_scroll, delta, lines, self.info_view);
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

    fn open_signals(&mut self) {
        let Some((stream_id, dataset_id)) = self
            .selected_dataset()
            .map(|dataset| (dataset.datastream_id, dataset.id))
        else {
            return;
        };
        self.browse_level = BrowseLevel::Datasets;
        self.focus = Focus::Table;
        self.status.clear();
        self.request_signals(stream_id, dataset_id);
    }

    fn maybe_autoload_datasets(&mut self) {
        let Some((id, n_datasets)) = self
            .selected_stream()
            .map(|stream| (stream.id, stream.n_datasets))
        else {
            return;
        };
        if is_cheap(n_datasets) {
            self.request_datasets(id);
        }
    }

    fn maybe_autoload_signals(&mut self) {
        let Some((stream_id, dataset_id, n_signals)) = self
            .selected_dataset()
            .map(|dataset| (dataset.datastream_id, dataset.id, dataset.n_signals))
        else {
            return;
        };
        if is_cheap(n_signals) {
            self.request_signals(stream_id, dataset_id);
        }
    }

    fn request_datasets(&mut self, stream_id: i64) {
        if !self.connected {
            self.prompt_for_env();
            return;
        }
        if self.loaded_stream_id == Some(stream_id) || self.loading_datasets == Some(stream_id) {
            return;
        }
        self.loading_datasets = Some(stream_id);
        self.load_tick = 0;
    }

    fn request_signals(&mut self, stream_id: i64, dataset_id: i64) {
        if !self.connected {
            self.prompt_for_env();
            return;
        }
        if self.signals_dataset_id == Some(dataset_id)
            || self.loading_signals == Some((stream_id, dataset_id))
        {
            return;
        }
        self.loading_signals = Some((stream_id, dataset_id));
        self.load_tick = 0;
    }

    fn has_pending_load(&self) -> bool {
        self.loading_datasets.is_some() || self.loading_signals.is_some()
    }

    fn loading_dots(&self) -> &'static str {
        spinner_frame(self.load_tick)
    }

    fn is_loading_datasets(&self) -> bool {
        let Some(id) = self.selected_stream().map(|stream| stream.id) else {
            return false;
        };
        self.loading_datasets == Some(id)
    }

    fn is_loading_signals(&self) -> bool {
        let Some(id) = self.selected_dataset().map(|dataset| dataset.id) else {
            return false;
        };
        self.loading_signals
            .is_some_and(|(_, dataset_id)| dataset_id == id)
    }

    fn apply_load_result(&mut self, result: Result<LoadResult, String>) {
        match result {
            Ok(LoadResult::Datasets(stream_id, datasets)) => {
                self.loading_datasets = None;
                self.apply_datasets(stream_id, datasets);
            }
            Ok(LoadResult::Signals(dataset_id, signals)) => {
                self.loading_signals = None;
                self.apply_signals(dataset_id, signals);
            }
            Err(error) => {
                self.loading_datasets = None;
                self.loading_signals = None;
                self.status = error;
            }
        }
    }

    fn apply_datasets(&mut self, stream_id: i64, mut datasets: Vec<Dataset>) {
        datasets.sort_by_key(|dataset| std::cmp::Reverse(dataset.id));
        self.datasets = datasets;
        self.signals.clear();
        self.signals_dataset_id = None;
        self.loaded_stream_id = Some(stream_id);
        self.dataset_state.select(if self.datasets.is_empty() {
            None
        } else {
            Some(0)
        });
    }

    fn apply_signals(&mut self, dataset_id: i64, mut signals: Vec<Signal>) {
        signals.sort_by(|left, right| left.name.cmp(&right.name));
        self.signals = signals;
        self.signals_dataset_id = Some(dataset_id);
        self.signal_state.select(if self.signals.is_empty() {
            None
        } else {
            Some(0)
        });
    }

    async fn ensure_datasets(&mut self, stream_id: i64) -> std::result::Result<(), String> {
        if !self.connected {
            self.prompt_for_env();
            return Err(self.status.clone());
        }
        if self.loaded_stream_id == Some(stream_id) {
            return Ok(());
        }
        match self.db.get_datasets(stream_id).await {
            Ok(datasets) => {
                self.apply_datasets(stream_id, datasets);
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    async fn use_env_file(&mut self, path: PathBuf) {
        match apply_env_file(&path) {
            Ok((url, token)) => match connect(&url, &token) {
                Ok(db) => {
                    self.db = db;
                    self.url = url;
                    self.env_file = Some(path.clone());
                    self.clear_loaded();
                    self.browse_level = BrowseLevel::Root;
                    self.focus = Focus::Table;
                    self.info_expanded = false;
                    self.info_scroll = 0;
                    self.refresh_streams().await;
                    if self.connected {
                        self.remember_current_env();
                        self.env_picker = None;
                        self.status.clear();
                    }
                    self.persist_settings();
                }
                Err(error) => self.status = error.to_string(),
            },
            Err(error) => self.status = error.to_string(),
        }
    }

    fn clear_loaded(&mut self) {
        self.datasets.clear();
        self.signals.clear();
        self.loaded_stream_id = None;
        self.signals_dataset_id = None;
        self.loading_datasets = None;
        self.loading_signals = None;
    }

    fn selected_stream(&self) -> Option<&Stream> {
        self.stream_state
            .selected()
            .and_then(|index| self.streams.get(index))
    }

    fn loaded_stream(&self) -> Option<&Stream> {
        self.loaded_stream_id
            .and_then(|id| self.streams.iter().find(|stream| stream.id == id))
    }

    fn table_shows_current_stream(&self) -> bool {
        self.selected_stream().map(|stream| stream.id) == self.loaded_stream_id
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

    fn selected_dataset(&self) -> Option<&Dataset> {
        self.dataset_state
            .selected()
            .and_then(|index| self.datasets.get(index))
    }

    fn selected_signal(&self) -> Option<&Signal> {
        self.signal_state
            .selected()
            .and_then(|index| self.signals.get(index))
    }

    pub(super) fn stream_indices(&self, filtered: bool) -> Vec<usize> {
        self.indices(filtered, self.streams.len(), |index| {
            let stream = &self.streams[index];
            vec![
                stream.id.to_string(),
                stream_kind(stream).to_string(),
                stream.name.clone(),
                stream.plugin.clone().unwrap_or_default(),
                stream.plugin_args.clone().unwrap_or_default(),
                stream
                    .n_datasets
                    .map(|count| count.to_string())
                    .unwrap_or_default(),
                stream.description.clone(),
            ]
        })
    }

    pub(super) fn dataset_indices(&self, filtered: bool) -> Vec<usize> {
        self.indices(filtered, self.datasets.len(), |index| {
            let dataset = &self.datasets[index];
            vec![
                dataset.id.to_string(),
                dataset.path.clone(),
                crate::format_import_status(dataset.import_status).to_string(),
                dataset.import_message.clone().unwrap_or_default(),
                dataset
                    .n_signals
                    .map(|count| count.to_string())
                    .unwrap_or_default(),
            ]
        })
    }

    pub(super) fn signal_indices(&self, filtered: bool) -> Vec<usize> {
        self.indices(filtered, self.signals.len(), |index| {
            let signal = &self.signals[index];
            vec![
                signal_kind(signal).to_string(),
                signal.id.to_string(),
                signal.name.clone(),
                signal.unit.clone().unwrap_or_default(),
                signal_source(signal).to_string(),
                signal.description.clone().unwrap_or_default(),
            ]
        })
    }

    fn indices(
        &self,
        filtered: bool,
        len: usize,
        fields_at: impl FnMut(usize) -> Vec<String>,
    ) -> Vec<usize> {
        if filtered && self.search.active() {
            filter_indices(len, &self.search.query, fields_at)
        } else {
            (0..len).collect()
        }
    }

    fn focused_visible(&self) -> Vec<usize> {
        match (self.browse_level, self.focus) {
            (BrowseLevel::Root, _) | (BrowseLevel::Streams, Focus::List) => {
                self.stream_indices(true)
            }
            (BrowseLevel::Streams, Focus::Table) | (BrowseLevel::Datasets, Focus::List) => {
                self.dataset_indices(true)
            }
            (BrowseLevel::Datasets, Focus::Table) => self.signal_indices(true),
        }
    }

    fn snap_search(&mut self) {
        let visible = self.focused_visible();
        self.apply_selection(|state, _| {
            state.select(snap_visible(&visible, state.selected()));
        });
    }

    fn move_sel(&mut self, delta: i32) {
        let visible = self.focused_visible();
        self.apply_selection(|state, _| {
            state.select(step_visible(&visible, state.selected(), delta));
        });
    }

    fn goto_sel(&mut self, index: usize) {
        let visible = self.focused_visible();
        self.apply_selection(|state, _| state.select(goto_visible(&visible, index)));
    }

    fn apply_selection(&mut self, select: impl FnOnce(&mut TableState, usize)) {
        if self.info_expanded {
            self.info_scroll = 0;
        }
        match (self.browse_level, self.focus) {
            (BrowseLevel::Root, _) => select(&mut self.stream_state, self.streams.len()),
            (BrowseLevel::Streams, Focus::List) => {
                select(&mut self.stream_state, self.streams.len());
                self.maybe_autoload_datasets();
            }
            (BrowseLevel::Datasets, Focus::List) => {
                select(&mut self.dataset_state, self.datasets.len());
                self.maybe_autoload_signals();
            }
            (BrowseLevel::Streams, Focus::Table) => {
                select(&mut self.dataset_state, self.datasets.len());
            }
            (BrowseLevel::Datasets, Focus::Table) => {
                select(&mut self.signal_state, self.signals.len());
            }
        }
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

    fn persist_settings(&self) {
        let settings = BrowseSettings {
            env_file: self.env_file.clone(),
            stream_id: self.loaded_stream_id,
            recents: self.recents.clone(),
        };
        let _ = save_settings(&settings);
    }

    async fn restore_session(&mut self, settings: &BrowseSettings) {
        if let Some(stream_id) = settings.stream_id
            && let Some(index) = self
                .streams
                .iter()
                .position(|stream| stream.id == stream_id)
        {
            self.stream_state.select(Some(index));
            if self.ensure_datasets(stream_id).await.is_ok() {
                self.browse_level = BrowseLevel::Streams;
                self.focus = Focus::List;
            }
        }
    }

    fn env_label(&self) -> String {
        self.env_file
            .as_deref()
            .map(env_label)
            .unwrap_or_else(|| "env".to_string())
    }

    fn loaded_import_mix(&self) -> Option<ImportMix> {
        let id = self.selected_stream()?.id;
        (self.loaded_stream_id == Some(id)).then(|| {
            let mut finished = 0;
            let mut live = 0;
            let mut failed = 0;
            for dataset in &self.datasets {
                match dataset.import_status {
                    ImportStatus::Finished => finished += 1,
                    ImportStatus::Live => live += 1,
                    status if status.is_failure() => failed += 1,
                    _ => {}
                }
            }
            ImportMix {
                finished,
                live,
                failed,
                total: self.datasets.len(),
            }
        })
    }

    fn info_for_inspect(&self) -> (String, Vec<Line<'static>>) {
        match (self.browse_level, self.focus) {
            (BrowseLevel::Root, _) | (BrowseLevel::Streams, Focus::List) => stream_info(
                self.selected_stream(),
                true,
                self.loaded_import_mix().map(|mix| (mix.finished, mix.live)),
            ),
            (BrowseLevel::Streams, Focus::Table) | (BrowseLevel::Datasets, Focus::List) => {
                dataset_info(self.selected_dataset(), true)
            }
            (BrowseLevel::Datasets, Focus::Table) => signal_info(self.selected_signal()),
        }
    }
}

fn clamp_scroll(scroll: u16, delta: i32, lines: u16, view: u16) -> u16 {
    let max = lines.saturating_sub(view.saturating_sub(2).max(1));
    (scroll as i32 + delta).clamp(0, max as i32) as u16
}

fn spinner_frame(tick: u8) -> &'static str {
    SPINNER[(tick as usize) % SPINNER.len()]
}

#[cfg(test)]
mod tests {
    use super::spinner_frame;

    #[test]
    fn spinner_cycles_dots() {
        assert_eq!(spinner_frame(0), ".");
        assert_eq!(spinner_frame(1), "..");
        assert_eq!(spinner_frame(2), "...");
        assert_eq!(spinner_frame(3), ".");
    }
}
