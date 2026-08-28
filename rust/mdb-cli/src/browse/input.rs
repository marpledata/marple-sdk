use super::picker::FilePicker;
use super::upload::FormFocus;
use super::{App, BrowseLevel, Motion, PAGE_SIZE, cycle_focus};
use crate::table::{SearchAction, handle_search_key};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum InputMode {
    Browse,
    Info,
    Search,
    Env {
        editing: bool,
    },
    Upload {
        editing: bool,
        files: bool,
        submit: bool,
    },
    Download {
        editing: bool,
    },
}

impl App {
    pub(super) fn input_mode(&self) -> InputMode {
        if self
            .env_picker
            .as_ref()
            .is_some_and(|picker| picker.editing)
        {
            InputMode::Env { editing: true }
        } else if self.upload_typing() {
            InputMode::Upload {
                editing: true,
                files: false,
                submit: false,
            }
        } else if self.download_typing() {
            InputMode::Download { editing: true }
        } else if self.debug.search.editing || self.search.editing {
            InputMode::Search
        } else if self.env_picker.is_some() {
            InputMode::Env { editing: false }
        } else if let Some(form) = &self.upload.form {
            InputMode::Upload {
                editing: false,
                files: form.focus.is_files(),
                submit: form.focus == FormFocus::Upload,
            }
        } else if self.download.picker.is_some() {
            InputMode::Download { editing: false }
        } else if self.info_expanded || self.view_scrolls() {
            InputMode::Info
        } else {
            InputMode::Browse
        }
    }

    pub(super) fn help_text(&self) -> String {
        let env = self.env_label();
        match self.input_mode() {
            InputMode::Search if self.debug.search.editing => {
                format!(
                    "filter  /{}_  enter keep  esc cancel",
                    self.debug.search.query
                )
            }
            InputMode::Search => {
                format!("filter  /{}_  enter keep  esc cancel", self.search.query)
            }
            _ if !self.status.is_empty() => self.status.clone(),
            InputMode::Upload { editing: true, .. } => "enter keep  esc cancel".to_string(),
            InputMode::Upload { submit: true, .. } => {
                "enter upload  tab files  h/l field  esc close".to_string()
            }
            InputMode::Upload { files: false, .. } => {
                "tab files  h/l field  enter toggle/edit  esc close".to_string()
            }
            InputMode::Upload { .. } => {
                "tab footer  j/k  enter select  S-enter range  a all  → open  ← parent  / path  esc close"
                    .to_string()
            }
            InputMode::Download { editing: true } => "enter download here  esc cancel".to_string(),
            InputMode::Download { .. } => {
                "j/k  enter this folder  enter ../ up  → open  ← parent  / path  esc close"
                    .to_string()
            }
            InputMode::Env { editing: true } => "enter use or open  esc cancel".to_string(),
            InputMode::Env { .. } => {
                "j/k  tab recent|files  enter open/use  ← parent  / path  esc close".to_string()
            }
            InputMode::Info if self.info_expanded => {
                format!("j/k scroll  S-↓/↑ page  gg/G  i/esc close  w env ({env})  q quit")
            }
            InputMode::Info => {
                format!(
                    "j/k scroll  S-↓/↑ page  gg/G  → view  ← back  / filter  i info  w env ({env})  q quit"
                )
            }
            InputMode::Browse if self.browse_level == BrowseLevel::Root => {
                format!(
                    "j/k  S-↓/↑ page  gg/G  / filter  → open  i info  u upload  d download  x delete  r reingest  w env ({env})  q quit"
                )
            }
            InputMode::Browse if self.browse_level == BrowseLevel::Datasets => {
                format!(
                    "tab list|table  j/k  S-↓/↑ page  gg/G  / filter  → view  ← back  i info  u upload  d download  x delete  r reingest  w env ({env})  q quit"
                )
            }
            InputMode::Browse => {
                format!(
                    "tab list|table  j/k  S-↓/↑ page  gg/G  / filter  enter select  S-enter range  a all  → open  i info  u upload  d download  x delete  r reingest  ← back  w env ({env})  q quit"
                )
            }
        }
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(super) struct MotionState {
    count: Option<u32>,
    pending_g: bool,
}

impl MotionState {
    pub(super) fn pending(self) -> bool {
        self.count.is_some() || self.pending_g
    }

    pub(super) fn clear(&mut self) {
        *self = Self::default();
    }
}

#[derive(Debug)]
enum MotionRead {
    Pending,
    Act(Motion),
    None,
}

pub(super) async fn handle_key(app: &mut App, mut key: KeyEvent) -> bool {
    loop {
        if matches!(key.code, KeyCode::Esc) && app.motion.pending() {
            app.motion.clear();
            return false;
        }
        match app.input_mode() {
            InputMode::Env { editing: true } => {
                handle_env_input(app, key).await;
                return false;
            }
            InputMode::Upload { editing: true, .. } => {
                app.handle_upload_input(key);
                return false;
            }
            InputMode::Download { editing: true } => {
                app.handle_download_input(key);
                return false;
            }
            InputMode::Search => {
                if app.debug.search.editing {
                    handle_search_key(&mut app.debug.search, key);
                    app.debug.scroll = 0;
                } else if handle_search_key(&mut app.search, key) != SearchAction::Ignored {
                    app.snap_search();
                }
                return false;
            }
            mode => {
                if !matches!(mode, InputMode::Upload { editing: true, .. }) {
                    match read_motion(&mut app.motion, key) {
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
                }
                return match mode {
                    InputMode::Env { .. } => {
                        handle_env_key(app, key).await;
                        false
                    }
                    InputMode::Upload { .. } => {
                        app.handle_upload_key(key);
                        false
                    }
                    InputMode::Download { .. } => {
                        app.handle_download_key(key);
                        false
                    }
                    InputMode::Info | InputMode::Browse => handle_browse_key(app, key),
                    InputMode::Search => unreachable!(),
                };
            }
        }
    }
}

fn handle_browse_key(app: &mut App, key: KeyEvent) -> bool {
    if app.handle_confirm_key(key) {
        return false;
    }
    if matches!(key.code, KeyCode::Char('/')) {
        if app.debug_filter_enabled() {
            app.motion.clear();
            app.debug.search.start();
            return false;
        }
        if !app.info_expanded && !app.view_scrolls() {
            app.motion.clear();
            app.search.start();
            return false;
        }
    }
    if matches!(key.code, KeyCode::Esc) && app.debug.search.active() && app.debug_filter_enabled() {
        app.debug.search.clear();
        return false;
    }
    if matches!(key.code, KeyCode::Esc) && app.search.active() {
        app.search.clear();
        return false;
    }
    match key.code {
        KeyCode::Char('q') => return true,
        KeyCode::Tab | KeyCode::BackTab => {
            app.focus = cycle_focus(app.browse_level, app.focus);
            app.snap_search();
        }
        KeyCode::Esc | KeyCode::Char('h') | KeyCode::Left => app.go_back(),
        KeyCode::Enter if app.dataset_table_focused() && !app.info_expanded => {
            if key.modifiers.contains(KeyModifiers::SHIFT) {
                app.select_dataset_range();
            } else {
                app.toggle_dataset_selection();
            }
        }
        KeyCode::Char('l') | KeyCode::Right | KeyCode::Enter => app.activate(),
        KeyCode::Char('i') => app.toggle_info(),
        KeyCode::Char('w') => app.open_env(),
        KeyCode::Char('u') => app.open_upload(),
        KeyCode::Char('d') => app.open_download(),
        KeyCode::Char('x') => app.request_delete(),
        KeyCode::Char('r') => app.request_reingest(),
        KeyCode::Char(' ') => app.toggle_dataset_selection(),
        KeyCode::Char('a') => app.select_all_datasets(),
        _ => {}
    }
    false
}

async fn handle_env_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => app.env_picker = None,
        KeyCode::Enter => {
            if let Some(path) = app.env_picker.as_mut().and_then(FilePicker::enter_selected) {
                app.use_env_file(path);
            }
        }
        other => {
            let Some(picker) = app.env_picker.as_mut() else {
                return;
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
}

async fn handle_env_input(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Enter => {
            let result = app
                .env_picker
                .as_mut()
                .map(FilePicker::submit_input)
                .unwrap_or(Err("no picker".to_string()));
            match result {
                Ok(Some(path)) => app.use_env_file(path),
                Ok(None) => {}
                Err(error) => app.status = error,
            }
        }
        other => {
            let Some(picker) = app.env_picker.as_mut() else {
                return;
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
}

fn read_motion(state: &mut MotionState, key: KeyEvent) -> MotionRead {
    if let KeyCode::Char(c) = key.code
        && c.is_ascii_digit()
        && !key.modifiers.contains(KeyModifiers::SHIFT)
    {
        if c == '0' && state.count.is_none() && !state.pending_g {
            return MotionRead::Act(Motion::First);
        }
        let Some(digit) = c.to_digit(10) else {
            return MotionRead::None;
        };
        state.count = Some(
            state
                .count
                .unwrap_or(0)
                .saturating_mul(10)
                .saturating_add(digit),
        );
        return MotionRead::Pending;
    }

    let shift = key.modifiers.contains(KeyModifiers::SHIFT);
    let count = state.count.unwrap_or(1) as i32;
    let motion = match key.code {
        KeyCode::Char('g') if !shift => {
            if state.pending_g {
                let n = state.count.take().unwrap_or(1);
                state.pending_g = false;
                return MotionRead::Act(Motion::Goto(n.saturating_sub(1) as usize));
            }
            state.pending_g = true;
            return MotionRead::Pending;
        }
        KeyCode::Char('G') => match state.count.take() {
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
            state.clear();
            return MotionRead::None;
        }
    };
    state.clear();
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
    match app.input_mode() {
        InputMode::Env { .. } => apply_env_motion(app, motion),
        InputMode::Upload { .. } => app.apply_upload_motion(motion),
        InputMode::Download { .. } => app.apply_download_motion(motion),
        InputMode::Info => apply_info_motion(app, motion),
        InputMode::Browse => apply_browse_motion(app, motion),
        _ => {}
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
        Motion::Page(pages) => app.move_sel(pages * PAGE_SIZE),
        Motion::First => app.goto_sel(0),
        Motion::Last => app.goto_sel(usize::MAX),
        Motion::Goto(index) => app.goto_sel(index),
    }
}

fn apply_info_motion(app: &mut App, motion: Motion) {
    match motion {
        Motion::Delta(delta) => app.scroll_info(delta),
        Motion::Page(pages) => app.scroll_info(pages * PAGE_SIZE),
        Motion::First => app.reset_view_scroll(),
        Motion::Last => app.scroll_info(i32::MAX),
        Motion::Goto(index) => {
            app.reset_view_scroll();
            app.scroll_info(index as i32);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Motion, MotionRead, MotionState, read_motion};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn shift(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::SHIFT)
    }

    fn act(state: &mut MotionState, code: KeyCode) -> Motion {
        match read_motion(state, key(code)) {
            MotionRead::Act(motion) => motion,
            other => panic!("expected Act, got {other:?}"),
        }
    }

    #[test]
    fn digits_prefix_then_j_moves_by_count() {
        let mut state = MotionState::default();
        assert!(matches!(
            read_motion(&mut state, key(KeyCode::Char('1'))),
            MotionRead::Pending
        ));
        assert!(matches!(
            read_motion(&mut state, key(KeyCode::Char('2'))),
            MotionRead::Pending
        ));
        assert!(matches!(
            act(&mut state, KeyCode::Char('j')),
            Motion::Delta(12)
        ));
        assert!(!state.pending());
    }

    #[test]
    fn leading_zero_goes_first_gg_and_count_gg_goto() {
        let mut state = MotionState::default();
        assert!(matches!(act(&mut state, KeyCode::Char('0')), Motion::First));

        assert!(matches!(
            read_motion(&mut state, key(KeyCode::Char('g'))),
            MotionRead::Pending
        ));
        assert!(matches!(
            act(&mut state, KeyCode::Char('g')),
            Motion::Goto(0)
        ));

        assert!(matches!(
            read_motion(&mut state, key(KeyCode::Char('4'))),
            MotionRead::Pending
        ));
        assert!(matches!(
            read_motion(&mut state, key(KeyCode::Char('g'))),
            MotionRead::Pending
        ));
        assert!(matches!(
            act(&mut state, KeyCode::Char('g')),
            Motion::Goto(3)
        ));
    }

    #[test]
    fn uppercase_g_last_or_goto_and_shift_arrows_page() {
        let mut state = MotionState::default();
        assert!(matches!(act(&mut state, KeyCode::Char('G')), Motion::Last));
        assert!(matches!(
            read_motion(&mut state, key(KeyCode::Char('3'))),
            MotionRead::Pending
        ));
        assert!(matches!(
            act(&mut state, KeyCode::Char('G')),
            Motion::Goto(2)
        ));
        assert!(matches!(
            read_motion(&mut state, shift(KeyCode::Down)),
            MotionRead::Act(Motion::Page(1))
        ));
        assert!(matches!(
            read_motion(&mut state, shift(KeyCode::Up)),
            MotionRead::Act(Motion::Page(-1))
        ));
    }

    #[test]
    fn unrelated_key_clears_pending_count() {
        let mut state = MotionState::default();
        assert!(matches!(
            read_motion(&mut state, key(KeyCode::Char('5'))),
            MotionRead::Pending
        ));
        assert!(matches!(
            read_motion(&mut state, key(KeyCode::Char('x'))),
            MotionRead::None
        ));
        assert!(!state.pending());
    }
}
