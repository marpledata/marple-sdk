# Marple MATLAB SDK

This folder contains a small MATLAB client for Marple DB.

## Quickstart

1. Open `matlab/config.json` and fill in:

   - `api_key`
   - `workspace`
   - (optional) `api_url`
   - (optional) `datapool`

2. In MATLAB, add this folder to your path:

   ```matlab
   addpath(genpath(fullfile(pwd, 'matlab')));
   ```

3. Create the client (reads `config.json` next to `DB.m`):

   ```matlab
   mdb = DB.from_config();
   ```

4. Run the example script:

   ```matlab
   run(fullfile('matlab', 'example.m'));
   ```

## Notes

- `DB.from_config()` reads `config.json` next to `DB.m`. `api_key` is sent as a Bearer token to the Marple DB API.
- `get_data(dataset_path, signal_name)` fetches a list of parquet URLs from the API, downloads them into `_marplecache/<workspace>/<datapool>/dataset=<id>/signal=<id>/` (via `websave`), and reads them via `parquetDatastore(...)`.
- If you want a clean re-download, call `mdb.clear_cache()`.
- If you use MATLAB Online, make sure this `matlab/` folder is on your path and that your `config.json` is set appropriately.

## Compatibility

MATLAB versions older than R2023a do not support reading Parquet files compressed with zstd. To work around this, the SDK automatically downloads a small helper binary (`parquet-transcode`) on first use, which re-compresses cached Parquet files from zstd to Snappy before MATLAB reads them.

- The binary is downloaded once into `matlab/bin/` and reused on subsequent calls.
- Already-transcoded files (Snappy or uncompressed) are detected and skipped, so there is no overhead on repeated reads.
- Supported platforms: Windows x64, macOS ARM (Apple Silicon), macOS x64 (Intel), Linux x64.
- Internet access is required on first use to download the binary (~3-5 MB).
