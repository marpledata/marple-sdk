use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

const SAAS_URL: &str = "https://db.marpledata.com/api/v1";

#[derive(Clone, Debug)]
pub(crate) struct EnvChoice {
    pub path: PathBuf,
    pub label: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct TuiSettings {
    pub env_file: Option<PathBuf>,
    #[serde(default)]
    pub stream_id: Option<i32>,
}

fn xdg_home(var: &str, fallback: &str) -> PathBuf {
    std::env::var_os(var)
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(fallback)))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn config_dir() -> PathBuf {
    xdg_home("XDG_CONFIG_HOME", ".config").join("mdb")
}

pub(crate) fn settings_path() -> PathBuf {
    config_dir().join("tui.toml")
}

pub(crate) fn load_settings() -> TuiSettings {
    fs::read_to_string(settings_path())
        .ok()
        .and_then(|body| toml::from_str(&body).ok())
        .unwrap_or_default()
}

pub(crate) fn save_settings(settings: &TuiSettings) -> Result<()> {
    let path = settings_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, toml::to_string_pretty(settings)?)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

pub(crate) fn apply_env_file(path: &Path) -> Result<(String, String)> {
    dotenvy::from_path_override(path)
        .with_context(|| format!("failed to load env file {}", path.display()))?;
    let url = std::env::var("MDB_URL").unwrap_or_else(|_| SAAS_URL.to_string());
    let token = std::env::var("MDB_TOKEN").unwrap_or_default();
    if token.is_empty() {
        bail!("{} does not set MDB_TOKEN", path.display());
    }
    Ok((url, token))
}

pub(crate) fn local_dotenv() -> Option<PathBuf> {
    std::env::current_dir()
        .ok()
        .map(|cwd| cwd.join(".env"))
        .filter(|path| path.is_file())
}

pub(crate) fn discover_env_files(saved: Option<&Path>) -> Vec<EnvChoice> {
    discover_env_files_in(std::env::current_dir().ok().as_deref(), saved)
}

fn discover_env_files_in(cwd: Option<&Path>, saved: Option<&Path>) -> Vec<EnvChoice> {
    let mut choices = Vec::new();
    let mut seen = HashSet::new();
    let mut push = |path: PathBuf| {
        if path.is_file() {
            let key = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
            if seen.insert(key) {
                choices.push(EnvChoice {
                    label: env_label(&path),
                    path,
                });
            }
        }
    };
    if let Some(cwd) = cwd {
        push(cwd.join(".env"));
    }
    if let Some(saved) = saved {
        push(saved.to_path_buf());
    }
    choices
}

fn env_label(path: &Path) -> String {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(".env");
    if let Some(parent) = path.parent().and_then(|parent| parent.file_name()) {
        format!("{}/{}", parent.to_string_lossy(), name)
    } else {
        name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{TuiSettings, discover_env_files_in, load_settings, save_settings};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_xdg(var: &str, path: &Path, body: impl FnOnce()) {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let previous = std::env::var_os(var);
        unsafe {
            std::env::set_var(var, path);
        }
        body();
        unsafe {
            match previous {
                Some(value) => std::env::set_var(var, value),
                None => std::env::remove_var(var),
            }
        }
    }

    #[test]
    fn persists_env_file_in_xdg_config() {
        let tmp = tempfile::tempdir().unwrap();
        with_xdg("XDG_CONFIG_HOME", tmp.path(), || {
            let settings = TuiSettings {
                env_file: Some(PathBuf::from("/tmp/custom.env")),
                stream_id: Some(5),
            };
            save_settings(&settings).unwrap();
            let loaded = load_settings();
            assert_eq!(
                loaded.env_file.as_deref(),
                Some(Path::new("/tmp/custom.env"))
            );
            assert_eq!(loaded.stream_id, Some(5));
            assert!(tmp.path().join("mdb/tui.toml").is_file());
        });
    }

    #[test]
    fn discovers_cwd_dotenv_and_saved_file() {
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join(".env");
        fs::write(&local, "MDB_TOKEN=local\n").unwrap();
        let saved = tmp.path().join("custom.env");
        fs::write(&saved, "MDB_TOKEN=saved\n").unwrap();

        let both = discover_env_files_in(Some(tmp.path()), Some(&saved));
        assert_eq!(both.len(), 2);
        assert_eq!(both[0].path, local);
        assert_eq!(both[1].path, saved);

        let empty = tempfile::tempdir().unwrap();
        assert!(discover_env_files_in(Some(empty.path()), None).is_empty());
        assert_eq!(
            discover_env_files_in(Some(empty.path()), Some(&saved)).len(),
            1
        );
        assert_eq!(
            discover_env_files_in(Some(tmp.path()), Some(&local)).len(),
            1
        );
        assert!(
            discover_env_files_in(
                Some(tmp.path()),
                Some(empty.path().join("missing").as_path())
            )
            .len()
                == 1
        );
    }
}
