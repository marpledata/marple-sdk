---
name: marple-sdk
description: Build apps, scripts, and visualizations on Marple DB time series data using the marpledata Python SDK. Use when the user mentions Marple, marpledata, Marple DB, Marple Insight, the Marple API/REST/SDK, or wants to upload/import measurement files, query datasets, resample signals, or analyze/visualize time series stored in Marple.
---

# Marple SDK

Marpledata has two products:

- **Marple DB** — a high-performance time series lakehouse (built on Iceberg + PostgreSQL) for processing and standardising measurement-file data at extreme sizes and frequencies. Main target for SDK/programmatic access.
- **Marple Insight** — the analysis/dashboarding web product; secondary for the SDK.

The `marpledata` Python SDK is typed and docstringed, so discover functionality by introspecting the installed package and querying live docs rather than guessing.

## Install and authenticate

```bash
pip install marpledata   # or: uv add marpledata
```

```python
import os
from marple import DB

# Token is created in the Marple DB UI (Settings -> API Tokens), shown once.
db = DB(os.environ["MDB_TOKEN"])  # defaults to SaaS URL
db.check_connection()
```

- Token via `MDB_TOKEN` env var (the repo's convention; use `MDB_URL` for non-SaaS); never hardcode it.
- Default URL is SaaS (`https://db.marpledata.com/api/v1`). Pass a second arg `DB(token, api_url)` for VPC/self-hosted.

## Happy path: question to script to visualization

A general analytical question (e.g. "what happened in dataset x while event y?") is enough to produce a self-contained script that connects, pulls the relevant signals/window, and renders a visualization. Default approach:

1. Write a single-file `uv` script with PEP 723 inline dependencies (`marpledata`,`pandas`, etc.). Fall back to `requirements.txt` if `uv` is unavailable.
2. Read the token from `MDB_TOKEN`.
3. Connect, locate the dataset/stream, fetch + resample the signals of interest, use this data to fulfill the user's request

Do not over-engineer. Use judgement together with the user (and a plan for non-trivial apps) to refine which signals, time window, and visualisation types are actually needed. See a full recipe in `examples.md`.

## Discovering functionality

- Prefer typed built-in SDK methods. Find them by introspecting the installed package (read its source, docstrings, and type hints).
- Use raw calls like `db.get(...)` / `db.post(...)` only when no built-in method exists.
- When unsure, query the live docs on demand: `GET <doc-url>.md?ask=<question>&goal=<goal>` (GitBook answers from the docs). Example: `https://docs.marpledata.com/docs/sdk/overview/python-sdk.md?ask=how%20do%20I%20list%20datasets`.

## Gotchas

- A 403 means the token is invalid/expired or belongs to a different deployment. Verify the token; if it is valid, ask whether they are on SaaS, VPC, or self-hosted and set `api_url`/`MDB_URL` accordingly.
- Importing is async: after `stream.push_file(...)`, call `dataset.wait_for_import(timeout=...)` before reading data.
- Terminology: a `stream` contains `dataset`s; each dataset has `signal`s.

## More

- Links, other language targets (Rust, MATLAB, REST), and the reusable "Marple guidance" prompt block: `reference.md`
- End-to-end recipes (import, filter + resample, happy-path script): `examples.md`
