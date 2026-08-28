use super::session::RecentEnv;
use crate::table::wrap_index;
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub(crate) struct PickerEntry {
    pub name: String,
    pub path: PathBuf,
    pub is_dir: bool,
    pub workspace: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct FilePicker {
    pub dir: PathBuf,
    pub recents: Vec<PickerEntry>,
    pub entries: Vec<PickerEntry>,
    pub selected: usize,
    pub input: String,
    pub editing: bool,
    recents_offset: Cell<usize>,
    files_offset: Cell<usize>,
}

impl FilePicker {
    pub(crate) fn open(start: Option<&Path>, recents: &[RecentEnv]) -> Self {
        let dir = start_dir(start);
        let mut picker = Self {
            dir: PathBuf::new(),
            recents: recent_entries(recents),
            entries: Vec::new(),
            selected: 0,
            input: start
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
            editing: false,
            recents_offset: Cell::new(0),
            files_offset: Cell::new(0),
        };
        picker.set_dir(dir, start);
        picker
    }

    pub(crate) fn len(&self) -> usize {
        self.recents.len() + self.entries.len()
    }

    fn set_dir(&mut self, dir: PathBuf, select: Option<&Path>) {
        let dir = fs::canonicalize(&dir).unwrap_or(dir);
        self.entries = list_dir(&dir);
        self.dir = dir;
        self.selected = select
            .and_then(|target| self.index_of(target))
            .unwrap_or_else(|| self.recents.len().min(self.len().saturating_sub(1)));
        self.files_offset.set(0);
        if !self.editing {
            self.sync_input();
        }
    }

    pub(crate) fn move_sel(&mut self, delta: i32) {
        if self.len() == 0 {
            self.selected = 0;
            return;
        }
        let current = self.selected as i32;
        self.selected = wrap_index(self.len(), current, delta);
        if !self.editing {
            self.sync_input();
        }
    }

    pub(crate) fn goto(&mut self, index: usize) {
        if self.len() == 0 {
            self.selected = 0;
            return;
        }
        self.selected = index.min(self.len() - 1);
        if !self.editing {
            self.sync_input();
        }
    }

    pub(crate) fn selected_entry(&self) -> Option<&PickerEntry> {
        self.item(self.selected)
    }

    fn items(&self) -> impl Iterator<Item = &PickerEntry> {
        self.recents.iter().chain(self.entries.iter())
    }

    pub(crate) fn item(&self, index: usize) -> Option<&PickerEntry> {
        self.recents
            .get(index)
            .or_else(|| self.entries.get(index.checked_sub(self.recents.len())?))
    }

    fn index_of(&self, target: &Path) -> Option<usize> {
        self.items()
            .position(|entry| same_path(&entry.path, target))
    }

    pub(crate) fn enter_selected(&mut self) -> Option<PathBuf> {
        self.take_selected(false)
    }

    /// Returns the selected file, or a directory when `submit_dirs` is set.
    ///
    /// `..` always navigates. Other directories navigate when `submit_dirs` is
    /// false.
    pub(crate) fn take_selected(&mut self, submit_dirs: bool) -> Option<PathBuf> {
        let entry = self.selected_entry()?;
        if entry.is_dir {
            if submit_dirs && entry.name != ".." {
                return Some(entry.path.clone());
            }
            let path = entry.path.clone();
            self.editing = false;
            self.set_dir(path, None);
            None
        } else {
            Some(entry.path.clone())
        }
    }

    pub(crate) fn go_parent(&mut self) {
        if let Some(parent) = parent_dir(&self.dir) {
            let current = self.dir.clone();
            self.editing = false;
            self.set_dir(parent, Some(&current));
        }
    }

    pub(crate) fn cycle_section(&mut self) {
        if self.recents.is_empty() || self.entries.is_empty() {
            return;
        }
        if self.selected < self.recents.len() {
            self.goto(self.recents.len());
        } else {
            self.goto(0);
        }
    }

    pub(crate) fn recents_offset(&self) -> usize {
        self.recents_offset.get()
    }

    pub(crate) fn set_recents_offset(&self, offset: usize) {
        self.recents_offset.set(offset);
    }

    pub(crate) fn files_offset(&self) -> usize {
        self.files_offset.get()
    }

    pub(crate) fn set_files_offset(&self, offset: usize) {
        self.files_offset.set(offset);
    }

    pub(crate) fn start_editing(&mut self) {
        self.editing = true;
        if self.input.is_empty() {
            self.sync_input();
        }
    }

    pub(crate) fn cancel_editing(&mut self) {
        self.editing = false;
        self.sync_input();
    }

    pub(crate) fn push_char(&mut self, ch: char) {
        self.input.push(ch);
    }

    pub(crate) fn backspace(&mut self) {
        self.input.pop();
    }

    pub(crate) fn submit_input(&mut self) -> Result<Option<PathBuf>, String> {
        self.submit_typed(false)
    }

    pub(crate) fn submit_typed(&mut self, allow_dir: bool) -> Result<Option<PathBuf>, String> {
        let path = expand_path(self.input.trim());
        if path.as_os_str().is_empty() {
            return Err("enter a path".to_string());
        }
        if path.is_dir() {
            self.editing = false;
            if allow_dir {
                return Ok(Some(path));
            }
            self.set_dir(path, None);
            return Ok(None);
        }
        if path.is_file() {
            self.editing = false;
            return Ok(Some(path));
        }
        Err(format!(
            "{} is not a {}",
            path.display(),
            if allow_dir { "file or folder" } else { "file" }
        ))
    }

    fn sync_input(&mut self) {
        self.input = self
            .selected_entry()
            .map(|entry| entry.path.display().to_string())
            .unwrap_or_else(|| self.dir.display().to_string());
    }
}

fn recent_entries(recents: &[RecentEnv]) -> Vec<PickerEntry> {
    recents
        .iter()
        .filter(|recent| recent.path.is_file())
        .map(|recent| PickerEntry {
            name: recent
                .path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| recent.path.display().to_string()),
            path: recent.path.clone(),
            is_dir: false,
            workspace: Some(recent.workspace.clone()),
        })
        .collect()
}

fn start_dir(start: Option<&Path>) -> PathBuf {
    start
        .and_then(|path| {
            if path.is_file() {
                path.parent().map(Path::to_path_buf)
            } else if path.is_dir() {
                Some(path.to_path_buf())
            } else {
                path.parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .map(Path::to_path_buf)
            }
        })
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."))
}

fn parent_dir(dir: &Path) -> Option<PathBuf> {
    dir.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
}

fn list_dir(dir: &Path) -> Vec<PickerEntry> {
    let mut entries = Vec::new();
    if let Some(parent) = parent_dir(dir) {
        entries.push(PickerEntry {
            name: "..".to_string(),
            path: parent,
            is_dir: true,
            workspace: None,
        });
    }
    let mut children = Vec::new();
    if let Ok(read) = fs::read_dir(dir) {
        for entry in read.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "." || name == ".." {
                continue;
            }
            let path = entry.path();
            let is_dir = path.is_dir();
            children.push(PickerEntry {
                name,
                path,
                is_dir,
                workspace: None,
            });
        }
    }
    children.sort_by(|left, right| {
        right
            .is_dir
            .cmp(&left.is_dir)
            .then_with(|| left.name.to_lowercase().cmp(&right.name.to_lowercase()))
    });
    entries.extend(children);
    entries
}

fn expand_path(input: &str) -> PathBuf {
    if input == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(input));
    }
    if let Some(rest) = input.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(input)
}

pub(crate) fn path_key(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn same_path(left: &Path, right: &Path) -> bool {
    path_key(left) == path_key(right)
}

#[cfg(test)]
mod tests {
    use super::{FilePicker, expand_path, list_dir, parent_dir};
    use crate::browse::session::RecentEnv;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_home(path: &std::path::Path, body: impl FnOnce()) {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let previous = std::env::var_os("HOME");
        unsafe {
            std::env::set_var("HOME", path);
        }
        body();
        unsafe {
            match previous {
                Some(value) => std::env::set_var("HOME", value),
                None => std::env::remove_var("HOME"),
            }
        }
    }

    #[test]
    fn lists_dirs_then_files_and_opens_selection() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("python")).unwrap();
        fs::write(tmp.path().join(".env"), "MDB_TOKEN=local\n").unwrap();
        fs::write(tmp.path().join("python/.env.staging"), "MDB_TOKEN=stg\n").unwrap();

        let entries = list_dir(tmp.path());
        let names: Vec<_> = entries.iter().map(|entry| entry.name.as_str()).collect();
        assert!(names.contains(&"..") || parent_dir(tmp.path()).is_none());
        assert!(names.contains(&"python"));
        assert!(names.contains(&".env"));
        let python = names.iter().position(|name| *name == "python").unwrap();
        let env = names.iter().position(|name| *name == ".env").unwrap();
        assert!(python < env);

        let mut picker = FilePicker::open(Some(&tmp.path().join(".env")), &[]);
        assert_eq!(
            picker.selected_entry().map(|entry| entry.name.as_str()),
            Some(".env")
        );
        assert!(picker.enter_selected().is_some());

        picker = FilePicker::open(Some(tmp.path()), &[]);
        picker.goto(
            picker
                .entries
                .iter()
                .position(|entry| entry.name == "python")
                .unwrap(),
        );
        assert!(picker.enter_selected().is_none());
        assert!(
            picker
                .entries
                .iter()
                .any(|entry| entry.name == ".env.staging")
        );

        picker = FilePicker::open(Some(&tmp.path().join("python")), &[]);
        picker.goto(
            picker
                .entries
                .iter()
                .position(|entry| entry.name == "..")
                .unwrap(),
        );
        assert!(picker.enter_selected().is_none());
        assert!(picker.entries.iter().any(|entry| entry.name == ".env"));
    }

    #[test]
    fn typed_path_opens_dir_or_returns_file() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("custom.env");
        fs::write(&file, "MDB_TOKEN=saved\n").unwrap();
        let mut picker = FilePicker::open(Some(tmp.path()), &[]);
        picker.input = file.display().to_string();
        assert_eq!(picker.submit_input().unwrap(), Some(file));

        picker.input = tmp.path().display().to_string();
        assert_eq!(picker.submit_input().unwrap(), None);
        assert_eq!(picker.dir, fs::canonicalize(tmp.path()).unwrap());

        picker.input = tmp.path().join("missing.env").display().to_string();
        assert!(picker.submit_input().unwrap_err().contains("not a file"));
    }

    #[test]
    fn take_selected_can_submit_a_directory() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("data")).unwrap();
        fs::write(tmp.path().join("run.csv"), "time,value\n0,1\n").unwrap();
        let mut picker = FilePicker::open(Some(tmp.path()), &[]);
        picker.goto(
            picker
                .entries
                .iter()
                .position(|entry| entry.name == "data")
                .unwrap(),
        );
        let submitted = picker.take_selected(true);
        assert_eq!(
            submitted
                .as_deref()
                .and_then(|path| fs::canonicalize(path).ok()),
            fs::canonicalize(tmp.path().join("data")).ok()
        );
        assert_eq!(picker.dir, fs::canonicalize(tmp.path()).unwrap());

        picker.goto(
            picker
                .entries
                .iter()
                .position(|entry| entry.name == "run.csv")
                .unwrap(),
        );
        assert_eq!(
            picker
                .take_selected(true)
                .as_deref()
                .and_then(|path| fs::canonicalize(path).ok()),
            fs::canonicalize(tmp.path().join("run.csv")).ok()
        );
    }

    #[test]
    fn typed_path_can_submit_a_directory() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("data")).unwrap();
        let mut picker = FilePicker::open(Some(tmp.path()), &[]);
        picker.input = tmp.path().join("data").display().to_string();
        assert_eq!(
            picker.submit_typed(true).unwrap(),
            Some(tmp.path().join("data"))
        );
    }

    #[test]
    fn expand_path_resolves_home() {
        let tmp = tempfile::tempdir().unwrap();
        with_home(tmp.path(), || {
            assert_eq!(expand_path("~"), tmp.path());
            assert_eq!(expand_path("~/.env"), tmp.path().join(".env"));
            assert_eq!(expand_path("/abs/path.env"), PathBuf::from("/abs/path.env"));
        });
    }

    #[test]
    fn recents_sit_above_directory_listing() {
        let tmp = tempfile::tempdir().unwrap();
        let saved = tmp.path().join("saved.env");
        fs::write(&saved, "MDB_TOKEN=saved\n").unwrap();
        fs::write(tmp.path().join(".env"), "MDB_TOKEN=local\n").unwrap();
        let recents = [RecentEnv {
            path: saved.clone(),
            workspace: "Staging".to_string(),
        }];
        let picker = FilePicker::open(Some(&saved), &recents);
        assert_eq!(picker.recents[0].workspace.as_deref(), Some("Staging"));
        assert_eq!(picker.recents[0].name, "saved.env");
        assert!(picker.entries.iter().any(|entry| entry.name == ".env"));
        assert_eq!(
            picker.selected_entry().map(|entry| entry.path.as_path()),
            Some(saved.as_path())
        );

        let mut browsing = FilePicker::open(Some(tmp.path()), &recents);
        browsing.goto(browsing.recents.len());
        browsing.cycle_section();
        assert!(browsing.selected < browsing.recents.len());
        browsing.cycle_section();
        assert!(browsing.selected >= browsing.recents.len());
    }
}
