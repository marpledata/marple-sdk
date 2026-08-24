mod draw;
mod format;
mod session;

use crate::connect;
use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use draw::draw;
use format::{dataset_info, signal_info, stream_info};
use marple_db::{CurrentWorkspace, Dataset, ImportStatus, MarpleDB, Signal, Stream};
use ratatui::text::Line;
use ratatui::widgets::{ListState, TableState};
use session::{
    EnvChoice, TuiSettings, apply_env_file, discover_env_files, load_settings, local_dotenv,
    save_settings,
};
use std::path::{Path, PathBuf};

pub(super) const AUTO_LOAD_LIMIT: u64 = 100;
const PAGE_SIZE: i32 = 10;
const NOT_CONNECTED: &str = "not connected — pass --env-file PATH";
const PICK_ENV: &str = "not connected — pick an env file (v)";

pub(super) fn is_cheap(count: Option<u64>) -> bool {
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
    env_files: Vec<EnvChoice>,
    env_index: usize,
    streams: Vec<Stream>,
    datasets: Vec<Dataset>,
    signals: Vec<Signal>,
    loaded_stream_id: Option<i32>,
    signals_dataset_id: Option<i32>,
    stream_state: TableState,
    dataset_state: TableState,
    signal_state: TableState,
    env_state: ListState,
    browse_level: BrowseLevel,
    focus: Focus,
    show_env: bool,
    status: String,
    connected: bool,
    workspace: Option<CurrentWorkspace>,
    info_expanded: bool,
    info_scroll: u16,
    info_view: u16,
    motion_count: Option<u32>,
    pending_g: bool,
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
    let mut app = App::new(db, url, env_file);
    app.refresh_streams().await;
    app.restore_session(&settings).await;
    if !app.connected {
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

async fn handle_key(app: &mut App, key: KeyEvent) -> bool {
    if matches!(key.code, KeyCode::Esc) && app.has_pending_motion() {
        app.clear_motion();
        return false;
    }
    match read_motion(app, key) {
        MotionRead::Pending => return false,
        MotionRead::Act(motion) => {
            apply_motion(app, motion).await;
            return false;
        }
        MotionRead::None => {}
    }
    if app.show_env {
        return handle_env_key(app, key).await;
    }
    match key.code {
        KeyCode::Char('q') => return true,
        KeyCode::Tab | KeyCode::BackTab => app.focus = app.cycle_focus(),
        KeyCode::Esc | KeyCode::Char('h') | KeyCode::Left => app.go_back(),
        KeyCode::Char('l') | KeyCode::Right | KeyCode::Enter => app.activate().await,
        KeyCode::Char('i') => app.toggle_info(),
        KeyCode::Char('v') => app.open_env(),
        _ => {}
    }
    false
}

async fn handle_env_key(app: &mut App, key: KeyEvent) -> bool {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => app.show_env = false,
        KeyCode::Enter => {
            if let Some(index) = app.env_state.selected() {
                app.switch_env(index).await;
            }
            if app.connected {
                app.show_env = false;
            }
        }
        _ => {}
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

async fn apply_motion(app: &mut App, motion: Motion) {
    if app.show_env {
        apply_env_motion(app, motion);
    } else {
        apply_browse_motion(app, motion).await;
    }
}

fn apply_env_motion(app: &mut App, motion: Motion) {
    let len = app.env_files.len();
    match motion {
        Motion::Delta(delta) => {
            app.env_state
                .select(step_index(app.env_state.selected(), len, delta));
        }
        Motion::Page(pages) => {
            app.env_state
                .select(step_index(app.env_state.selected(), len, pages * PAGE_SIZE));
        }
        Motion::First => app.env_state.select(resolve(len, 0)),
        Motion::Last => app.env_state.select(resolve(len, len.saturating_sub(1))),
        Motion::Goto(index) => app.env_state.select(resolve(len, index)),
    }
}

async fn apply_browse_motion(app: &mut App, motion: Motion) {
    match motion {
        Motion::Delta(delta) => app.move_sel(delta).await,
        Motion::Page(pages) => {
            let delta = pages * PAGE_SIZE;
            if app.info_expanded {
                app.scroll_info(delta);
            } else {
                app.move_sel(delta).await;
            }
        }
        Motion::First => app.goto_sel(0).await,
        Motion::Last => app.goto_sel(usize::MAX).await,
        Motion::Goto(index) => app.goto_sel(index).await,
    }
}

impl App {
    fn new(db: MarpleDB, url: String, env_file: Option<PathBuf>) -> Self {
        let env_files = discover_env_files(env_file.as_deref());
        let env_index = env_file
            .as_ref()
            .and_then(|selected| {
                env_files
                    .iter()
                    .position(|choice| same_path(&choice.path, selected))
            })
            .unwrap_or(0);
        Self {
            db,
            url,
            env_file,
            env_files,
            env_index,
            streams: Vec::new(),
            datasets: Vec::new(),
            signals: Vec::new(),
            loaded_stream_id: None,
            signals_dataset_id: None,
            stream_state: TableState::default().with_selected(Some(0)),
            dataset_state: TableState::default(),
            signal_state: TableState::default(),
            env_state: ListState::default(),
            browse_level: BrowseLevel::Root,
            focus: Focus::Table,
            show_env: false,
            status: String::new(),
            connected: false,
            workspace: None,
            info_expanded: false,
            info_scroll: 0,
            info_view: 8,
            motion_count: None,
            pending_g: false,
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
        if self.env_files.is_empty() {
            self.status = NOT_CONNECTED.to_string();
        } else {
            self.status = PICK_ENV.to_string();
            self.open_env();
        }
    }

    fn open_env(&mut self) {
        if self.env_files.is_empty() {
            self.status = NOT_CONNECTED.to_string();
            return;
        }
        self.show_env = true;
        self.env_state.select(Some(self.env_index));
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
                self.status = format!("{} streams · {}", self.streams.len(), self.url);
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

    async fn activate(&mut self) {
        match (self.browse_level, self.focus) {
            (BrowseLevel::Root, _) | (BrowseLevel::Streams, Focus::List) => {
                self.load_stream_table().await
            }
            (BrowseLevel::Streams, Focus::Table) => {
                if self.table_shows_current_stream() {
                    self.show_signals().await;
                } else {
                    self.load_stream_table().await;
                }
            }
            (BrowseLevel::Datasets, Focus::List) => self.show_signals().await,
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

    async fn load_stream_table(&mut self) {
        let Some((id, name)) = self
            .selected_stream()
            .map(|stream| (stream.id, stream.name.clone()))
        else {
            return;
        };
        if let Err(error) = self.ensure_datasets(id).await {
            self.status = error;
            return;
        }
        self.browse_level = BrowseLevel::Streams;
        self.focus = Focus::Table;
        let finished = self
            .datasets
            .iter()
            .filter(|dataset| dataset.import_status == ImportStatus::Finished)
            .count();
        let live = self
            .datasets
            .iter()
            .filter(|dataset| dataset.import_status == ImportStatus::Live)
            .count();
        self.status = format!(
            "/{name} · {} datasets · FINISHED {finished} · LIVE {live}",
            self.datasets.len(),
        );
    }

    async fn show_signals(&mut self) {
        let Some(dataset) = self.selected_dataset().cloned() else {
            return;
        };
        if let Err(error) = self.ensure_signals(&dataset).await {
            self.status = error;
            return;
        }
        self.browse_level = BrowseLevel::Datasets;
        self.focus = Focus::Table;
        self.status = format!(
            "{} · {} signals",
            self.breadcrumb_path(),
            self.signals.len()
        );
    }

    async fn maybe_autoload_datasets(&mut self) {
        let Some((id, n_datasets)) = self
            .selected_stream()
            .map(|stream| (stream.id, stream.n_datasets))
        else {
            return;
        };
        if is_cheap(n_datasets)
            && let Err(error) = self.ensure_datasets(id).await
        {
            self.status = error;
        }
    }

    async fn maybe_autoload_signals(&mut self) {
        let Some(dataset) = self.selected_dataset().cloned() else {
            return;
        };
        if is_cheap(dataset.n_signals)
            && let Err(error) = self.ensure_signals(&dataset).await
        {
            self.status = error;
        }
    }

    async fn ensure_datasets(&mut self, stream_id: i32) -> std::result::Result<(), String> {
        if !self.connected {
            self.prompt_for_env();
            return Err(self.status.clone());
        }
        if self.loaded_stream_id == Some(stream_id) {
            return Ok(());
        }
        match self.db.get_datasets(stream_id).await {
            Ok(datasets) => {
                self.datasets = datasets;
                self.datasets.sort_by(|left, right| right.id.cmp(&left.id));
                self.signals.clear();
                self.signals_dataset_id = None;
                self.loaded_stream_id = Some(stream_id);
                self.dataset_state.select(if self.datasets.is_empty() {
                    None
                } else {
                    Some(0)
                });
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    async fn ensure_signals(&mut self, dataset: &Dataset) -> std::result::Result<(), String> {
        if !self.connected {
            self.prompt_for_env();
            return Err(self.status.clone());
        }
        if self.signals_dataset_id == Some(dataset.id) {
            return Ok(());
        }
        self.status = format!("loading signals for {}…", dataset.path);
        match self.db.get_signals(dataset.datastream_id, dataset.id).await {
            Ok(signals) => {
                self.signals = signals;
                self.signals
                    .sort_by(|left, right| left.name.cmp(&right.name));
                self.signals_dataset_id = Some(dataset.id);
                self.signal_state.select(if self.signals.is_empty() {
                    None
                } else {
                    Some(0)
                });
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    async fn switch_env(&mut self, index: usize) {
        let Some(choice) = self.env_files.get(index).cloned() else {
            return;
        };
        match apply_env_file(&choice.path) {
            Ok((url, token)) => match connect(&url, &token) {
                Ok(db) => {
                    self.db = db;
                    self.url = url;
                    self.env_file = Some(choice.path.clone());
                    self.env_index = index;
                    self.persist_settings();
                    self.clear_loaded();
                    self.browse_level = BrowseLevel::Root;
                    self.focus = Focus::Table;
                    self.info_expanded = false;
                    self.info_scroll = 0;
                    self.refresh_streams().await;
                    if self.connected {
                        self.status = format!("using {}", choice.label);
                    }
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

    async fn move_sel(&mut self, delta: i32) {
        self.apply_selection(|state, len| {
            state.select(step_index(state.selected(), len, delta));
        })
        .await;
    }

    async fn goto_sel(&mut self, index: usize) {
        self.apply_selection(|state, len| state.select(resolve(len, index)))
            .await;
    }

    async fn apply_selection(&mut self, select: impl FnOnce(&mut TableState, usize)) {
        if self.info_expanded {
            self.info_scroll = 0;
        }
        match (self.browse_level, self.focus) {
            (BrowseLevel::Root, _) => select(&mut self.stream_state, self.streams.len()),
            (BrowseLevel::Streams, Focus::List) => {
                select(&mut self.stream_state, self.streams.len());
                self.maybe_autoload_datasets().await;
            }
            (BrowseLevel::Datasets, Focus::List) => {
                select(&mut self.dataset_state, self.datasets.len());
                self.maybe_autoload_signals().await;
            }
            (BrowseLevel::Streams, Focus::Table) => {
                select(&mut self.dataset_state, self.datasets.len());
            }
            (BrowseLevel::Datasets, Focus::Table) => {
                select(&mut self.signal_state, self.signals.len());
            }
        }
    }

    fn persist_settings(&self) {
        let settings = TuiSettings {
            env_file: self.env_file.clone(),
            stream_id: self.loaded_stream_id,
        };
        let _ = save_settings(&settings);
    }

    async fn restore_session(&mut self, settings: &TuiSettings) {
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
            .as_ref()
            .map(|path| {
                self.env_files
                    .iter()
                    .find(|choice| same_path(&choice.path, path))
                    .map(|choice| choice.label.clone())
                    .unwrap_or_else(|| path.display().to_string())
            })
            .unwrap_or_else(|| "env".to_string())
    }

    fn info_for_highlight(&self) -> (String, Vec<Line<'static>>) {
        match self.browse_level {
            BrowseLevel::Root | BrowseLevel::Streams => stream_info(self.selected_stream(), false),
            BrowseLevel::Datasets => dataset_info(self.selected_dataset(), false),
        }
    }

    fn info_for_inspect(&self) -> (String, Vec<Line<'static>>) {
        match (self.browse_level, self.focus) {
            (BrowseLevel::Root, _) => stream_info(self.selected_stream(), true),
            (BrowseLevel::Streams, Focus::List) => stream_info(self.selected_stream(), true),
            (BrowseLevel::Streams, Focus::Table) => dataset_info(self.selected_dataset(), true),
            (BrowseLevel::Datasets, Focus::List) => dataset_info(self.selected_dataset(), true),
            (BrowseLevel::Datasets, Focus::Table) => signal_info(self.selected_signal()),
        }
    }
}

fn step_index(selected: Option<usize>, len: usize, delta: i32) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let current = selected.unwrap_or(0) as i32;
    Some((current + delta).clamp(0, len as i32 - 1) as usize)
}

fn resolve(len: usize, index: usize) -> Option<usize> {
    (len > 0).then(|| index.min(len - 1))
}

fn clamp_scroll(scroll: u16, delta: i32, lines: u16, view: u16) -> u16 {
    let max = lines.saturating_sub(view.saturating_sub(2).max(1));
    (scroll as i32 + delta).clamp(0, max as i32) as u16
}

fn same_path(left: &Path, right: &Path) -> bool {
    let canon = |path: &Path| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    canon(left) == canon(right)
}

#[cfg(test)]
mod tests {
    use super::{resolve, step_index};

    #[test]
    fn list_motion_clamps_instead_of_wrapping() {
        assert_eq!(step_index(Some(0), 5, -1), Some(0));
        assert_eq!(step_index(Some(4), 5, 1), Some(4));
        assert_eq!(step_index(Some(2), 5, 10), Some(4));
        assert_eq!(step_index(None, 5, 1), Some(1));
        assert_eq!(step_index(Some(0), 0, 1), None);
    }

    #[test]
    fn jump_is_one_based_count_clamped() {
        assert_eq!(resolve(10, 0), Some(0));
        assert_eq!(resolve(10, 9), Some(9));
        assert_eq!(resolve(10, 99), Some(9));
        assert_eq!(resolve(0, 0), None);
    }
}
