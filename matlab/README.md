# Marple MATLAB SDK

This folder contains a small MATLAB client for Marple DB.

## Quickstart

1. Open `matlab/config.json` and fill in:

   - `api_key`
   - `s3_access_key`, `s3_secret_key`
   - `workspace`, `datapool` (and optionally `api_url`, `s3_bucket`, `s3_region`)

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

- `DB.from_config()` sets AWS credentials via environment variables and uses `copyfile("s3://...")` + `parquetDatastore(...)` for fetching and reading data.
- If you use MATLAB Online, make sure this `matlab/` folder is on your path and that your credentials in `config.json` are set appropriately.
