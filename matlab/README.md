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
