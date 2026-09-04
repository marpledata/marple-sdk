use anyhow::{Context, Result, bail};
use marple_db::SAAS_URL;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

const RECENT_LIMIT: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecentEnv {
    pub path: PathBuf,
    pub workspace: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct BrowseSettings {
    pub env_file: Option<PathBuf>,
    #[serde(default)]
    pub recents: Vec<RecentEnv>,
    #[serde(default)]
    pub upload_dir: Option<PathBuf>,
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
    config_dir().join("browse.toml")
}

pub(crate) fn load_settings() -> BrowseSettings {
    fs::read_to_string(settings_path())
        .ok()
        .and_then(|body| toml::from_str(&body).ok())
        .unwrap_or_default()
}

pub(crate) fn save_settings(settings: &BrowseSettings) -> Result<()> {
    let path = settings_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, toml::to_string_pretty(settings)?)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

pub(crate) fn apply_env_file(path: &Path) -> Result<(String, String)> {
    let mut file_url = None;
    for item in dotenvy::from_path_iter(path)
        .with_context(|| format!("failed to load env file {}", path.display()))?
    {
        let (key, value) =
            item.with_context(|| format!("failed to load env file {}", path.display()))?;
        if key == "MDB_URL" {
            file_url = Some(value.clone());
        }
        unsafe {
            std::env::set_var(&key, &value);
        }
    }
    let url = file_url
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| SAAS_URL.to_string());
    unsafe {
        std::env::set_var("MDB_URL", &url);
    }
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
    use super::{
        BrowseSettings, RecentEnv, apply_env_file, load_settings, remember_recent, save_settings,
    };
    use marple_db::SAAS_URL;
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn restore_var(key: &str, previous: Option<OsString>) {
        unsafe {
            match previous {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }

    fn with_xdg(path: &Path, body: impl FnOnce()) {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let previous = std::env::var_os("XDG_CONFIG_HOME");
        unsafe {
            std::env::set_var("XDG_CONFIG_HOME", path);
        }
        body();
        restore_var("XDG_CONFIG_HOME", previous);
    }

    fn with_mdb_env(body: impl FnOnce()) {
        let _guard = ENV_LOCK.lock().expect("env lock");
        let previous_url = std::env::var_os("MDB_URL");
        let previous_token = std::env::var_os("MDB_TOKEN");
        body();
        restore_var("MDB_URL", previous_url);
        restore_var("MDB_TOKEN", previous_token);
    }

    #[test]
    fn persists_env_file_in_xdg_config() {
        let tmp = tempfile::tempdir().unwrap();
        with_xdg(tmp.path(), || {
            let settings = BrowseSettings {
                env_file: Some(PathBuf::from("/tmp/custom.env")),
                recents: vec![RecentEnv {
                    path: PathBuf::from("/tmp/custom.env"),
                    workspace: "Staging".to_string(),
                }],
                upload_dir: Some(PathBuf::from("/tmp/data")),
            };
            save_settings(&settings).unwrap();
            let loaded = load_settings();
            assert_eq!(
                loaded.env_file.as_deref(),
                Some(Path::new("/tmp/custom.env"))
            );
            assert_eq!(loaded.recents[0].workspace, "Staging");
            assert_eq!(loaded.upload_dir.as_deref(), Some(Path::new("/tmp/data")));
            assert!(tmp.path().join("mdb/browse.toml").is_file());
        });
    }

    #[test]
    fn ignores_legacy_stream_id_in_browse_toml() {
        let tmp = tempfile::tempdir().unwrap();
        with_xdg(tmp.path(), || {
            let dir = tmp.path().join("mdb");
            fs::create_dir_all(&dir).unwrap();
            fs::write(
                dir.join("browse.toml"),
                "stream_id = 5\nenv_file = \"/tmp/x.env\"\n",
            )
            .unwrap();
            let loaded = load_settings();
            assert_eq!(loaded.env_file.as_deref(), Some(Path::new("/tmp/x.env")));
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

    #[test]
    fn apply_env_file_without_url_defaults_to_saas() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(".env");
        fs::write(&path, "MDB_TOKEN=saas-token\n").unwrap();
        with_mdb_env(|| {
            unsafe {
                std::env::set_var("MDB_URL", "https://staging.example/api/v1");
                std::env::set_var("MDB_TOKEN", "staging-token");
            }
            let (url, token) = apply_env_file(&path).unwrap();
            assert_eq!(url, SAAS_URL);
            assert_eq!(token, "saas-token");
            assert_eq!(std::env::var("MDB_URL").unwrap(), SAAS_URL);
        });
    }

    #[test]
    fn apply_env_file_keeps_url_from_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(".env");
        fs::write(
            &path,
            "MDB_TOKEN=vpc-token\nMDB_URL=https://vpc.example/api/v1\n",
        )
        .unwrap();
        with_mdb_env(|| {
            unsafe {
                std::env::set_var("MDB_URL", "https://staging.example/api/v1");
            }
            let (url, token) = apply_env_file(&path).unwrap();
            assert_eq!(url, "https://vpc.example/api/v1");
            assert_eq!(token, "vpc-token");
        });
    }

    #[test]
    fn apply_env_file_empty_url_defaults_to_saas() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(".env");
        fs::write(&path, "MDB_TOKEN=saas-token\nMDB_URL=\n").unwrap();
        with_mdb_env(|| {
            let (url, token) = apply_env_file(&path).unwrap();
            assert_eq!(url, SAAS_URL);
            assert_eq!(token, "saas-token");
        });
    }
}
