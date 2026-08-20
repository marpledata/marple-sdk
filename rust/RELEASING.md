# Releasing Rust crates

Bump versions + changelogs on `main` first. Run from `rust/`.

## `marple-db` + `mdb-cli`

1. **Publish SDK** — `cargo publish -p marple-db`  
   Wait until crates.io indexes the new version (CLI depends on it).
2. **Publish CLI** — `cargo publish -p mdb-cli`
3. **GitHub** — tag `mdb-cli-vX.Y.Z`, create a Release, attach binaries from the GitLab `build:mdb-cli` job (`mdb-linux-x64`, `mdb-windows-x64.exe`; add darwin if you build it)

`marple-db` has no GitHub tag/release; its `CHANGELOG.md` is the history. CLI footnote links use `releases/tag/mdb-cli-v*`.

## `parquet-transcode` (only when the binary changes)

Tag `parquet-transcode-vX.Y.Z`, create a Release, attach binaries from GitLab `build:parquet-transcode`. Keep asset names aligned with `matlab/DB.m` (`TRANSCODE_VERSION`).
