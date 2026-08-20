# Releasing Rust crates

Bump versions + changelogs on `main` first. Run from `rust/`.

## `marple-db` + `mdb-cli`

1. **Publish SDK** — `cargo publish -p marple-db`
   Wait until crates.io indexes the new version (CLI depends on it).
2. **Publish CLI** — `cargo publish -p mdb-cli`
3. **GitHub** — tag `mdb-cli-vX.Y.Z`, create a Release, attach these zips.

```bash
export VERSION=X.Y.Z

# Unzip the GitLab artifact download from the repo root, then continue in rust/
unzip artifacts.zip
cd rust

# Linux
mv mdb-cli/artifacts/mdb-linux-x64 mdb-cli/artifacts/mdb
zip -j "mdb-v${VERSION}-linux-x64.zip" mdb-cli/artifacts/mdb mdb-cli/README.md mdb-cli/LICENSE

# Windows
mv mdb-cli/artifacts/mdb-windows-x64.exe mdb-cli/artifacts/mdb.exe
zip -j "mdb-v${VERSION}-windows-x64.zip" mdb-cli/artifacts/mdb.exe mdb-cli/README.md mdb-cli/LICENSE

# Darwin (local arm64 build)
zip -j "mdb-v${VERSION}-darwin-arm64.zip" target/release/mdb mdb-cli/README.md mdb-cli/LICENSE
```

`marple-db` has no GitHub tag/release; its `CHANGELOG.md` is the history. CLI footnote links use `releases/tag/mdb-cli-v*`.

## `parquet-transcode` (only when the binary changes)

Tag `parquet-transcode-vX.Y.Z`, create a Release, attach binaries from GitLab `build:parquet-transcode`. Keep asset names aligned with `matlab/DB.m` (`TRANSCODE_VERSION`).
