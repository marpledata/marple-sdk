use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

const SAAS_URL: &str = "https://db.marpledata.com/api/v1";
const RECENT_LIMIT: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecentEnv {
    pub path: PathBuf,
    pub workspace: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct TuiSettings {
    pub env_file: Option<PathBuf>,
    #[serde(default)]
    pub stream_id: Option<i32>,
    #[serde(default)]
    pub recents: Vec<RecentEnv>,
}

pub(crate) fn remember_recent(
    recents: Vec<RecentEnv>,
    path: PathBuf,
    workspace: String,
) -> Vec<RecentEnv> {
    let key = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
    let mut next = vec![RecentEnv { path, workspace }];
    for recent in recents {
        if !recent.path.is_file() {
            continue;
        }
        let other = fs::canonicalize(&recent.path).unwrap_or_else(|_| recent.path.clone());
        if other != key {
            next.push(recent);
        }
    }
    next.truncate(RECENT_LIMIT);
    next
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

pub(crate) fn env_label(path: &Path) -> String {
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
    use super::{RecentEnv, TuiSettings, load_settings, remember_recent, save_settings};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_xdg(path: &Path, body: impl FnOnce()) {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let previous = std::env::var_os("XDG_CONFIG_HOME");
        unsafe {
            std::env::set_var("XDG_CONFIG_HOME", path);
        }
        body();
        unsafe {
            match previous {
                Some(value) => std::env::set_var("XDG_CONFIG_HOME", value),
                None => std::env::remove_var("XDG_CONFIG_HOME"),
            }
        }
    }

    #[test]
    fn persists_env_file_in_xdg_config() {
        let tmp = tempfile::tempdir().unwrap();
        with_xdg(tmp.path(), || {
            let settings = TuiSettings {
                env_file: Some(PathBuf::from("/tmp/custom.env")),
                stream_id: Some(5),
                recents: vec![RecentEnv {
                    path: PathBuf::from("/tmp/custom.env"),
                    workspace: "Staging".to_string(),
                }],
            };
            save_settings(&settings).unwrap();
            let loaded = load_settings();
            assert_eq!(
                loaded.env_file.as_deref(),
                Some(Path::new("/tmp/custom.env"))
            );
            assert_eq!(loaded.stream_id, Some(5));
            assert_eq!(loaded.recents[0].workspace, "Staging");
            assert!(tmp.path().join("mdb/tui.toml").is_file());
        });
    }

    #[test]
    fn remember_recent_moves_to_front_and_drops_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let first = tmp.path().join("a.env");
        let second = tmp.path().join("b.env");
        let gone = tmp.path().join("missing.env");
        fs::write(&first, "a\n").unwrap();
        fs::write(&second, "b\n").unwrap();

        let recents = vec![
            RecentEnv {
                path: first.clone(),
                workspace: "One".to_string(),
            },
            RecentEnv {
                path: gone,
                workspace: "Gone".to_string(),
            },
        ];
        let recents = remember_recent(recents, second.clone(), "Two".to_string());
        assert_eq!(
            recents,
            vec![
                RecentEnv {
                    path: second,
                    workspace: "Two".to_string(),
                },
                RecentEnv {
                    path: first,
                    workspace: "One".to_string(),
                },
            ]
        );
    }
}
