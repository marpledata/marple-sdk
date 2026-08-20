# Releasing the MATLAB SDK

Bump `DB.VERSION` and `CHANGELOG.md` on `main` first.

1. **GitHub** — tag `matlab-vX.Y.Z`, create a Release (zip of `matlab/` is enough)

No package registry. If `parquet-transcode` binaries change, release that crate separately (`rust/RELEASING.md`).
