use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

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

pub(crate) fn git_root() -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("current directory")?;
    Ok(PathBuf::from(git_in(
        &cwd,
        &["rev-parse", "--show-toplevel"],
    )?))
}

fn git_in(dir: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .context("failed to run git")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {} failed: {}", args.join(" "), stderr.trim());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn xdg_home(var: &str, fallback: &str) -> PathBuf {
    std::env::var_os(var)
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(fallback)))
        .unwrap_or_else(|| PathBuf::from("."))
}

pub(crate) fn config_dir() -> PathBuf {
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

pub(crate) fn discover_env_files() -> Vec<EnvChoice> {
    let mut choices = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut dirs = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        dirs.push(cwd);
    }
    if let Ok(root) = git_root() {
        dirs.push(root.join("python"));
        dirs.push(root);
    }
    for dir in dirs {
        for name in [".env.staging", ".env.local", ".env.nightly", ".env"] {
            let path = dir.join(name);
            if path.is_file() {
                let key = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
                if seen.insert(key) {
                    choices.push(EnvChoice {
                        label: env_label(&path),
                        path,
                    });
                }
            }
        }
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
    use super::{TuiSettings, load_settings, save_settings};
    use std::path::{Path, PathBuf};

    fn with_xdg(var: &str, path: &Path, body: impl FnOnce()) {
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
}
