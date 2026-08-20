# Marple SDK reference

Pointers to canonical docs and how to explore them. This file intentionally does not copy the API; the SDK is typed and the docs are queryable, so explore on demand.

## Canonical links

Main path is the Python SDK on Marple DB; Rust and MATLAB pointers are included for other-language tasks.

- SDK API reference (versioned): https://marpledata.gitlab.io/marple-sdk/
- Python SDK overview: https://docs.marpledata.com/docs/sdk/overview/python-sdk
- Rust SDK (DB): crate `marple-db` (imported as `marple_db`), async/Tokio; CLI binary `mdb`. Source in `rust/`.
- MATLAB DB client: `matlab/DB.m` (+ `config.json`, `example.m`). Source in `matlab/`.
- Agentic coding guidance: https://docs.marpledata.com/docs/sdk/agentic-coding
- API tokens: https://docs.marpledata.com/docs/sdk/overview/api-tokens
- REST API overview: https://docs.marpledata.com/docs/sdk/overview/rest-api
- GitHub repo (read source for types/functions): https://github.com/marpledata/marple-sdk
- Marple DB Swagger: https://db.marpledata.com/api/docs

## How to explore

- Introspect the installed `marpledata` package: read its source, docstrings, and type hints to find the right method before writing raw calls.
- Query the docs live instead of preloading them. Append `.md` to any docs page URL and add an `ask` (and optional `goal`) query parameter:

```
GET https://docs.marpledata.com/docs/sdk/overview/python-sdk.md?ask=<question>&goal=<goal>
```

The response contains a direct answer plus relevant excerpts and sources. Use it when the answer is not on the current page or you need related sections.

## Marple guidance (reusable prompt block)

Paste this into a plan or prompt when building a Marple app:

```markdown
# Marple guidance
## Products and capabilities
Two products are available as core building blocks:
- Marple DB: High-performance Data Lakehouse for processing and standardising time series data coming from measurement files. Docs: https://docs.marpledata.com/docs/marple-db/welcome
- Marple Insight: Collaborative web interface for deep analysis, reporting and dashboarding on complex time series data. Docs: https://docs.marpledata.com/docs/marple-insight/welcome

## Programming language
- Python: preferred choice, works excellent with our Python SDK `marpledata` [Marple DB + Insight]
- MATLAB: SDK available [Marple DB only] — pull and push (files, signals)
- Rust: crate `marple-db` + CLI `mdb` [Marple DB only]
- Other: REST API available [Marple DB + Insight]

## Python SDK
- Installation: from PyPI as `marpledata` (Python >= 3.11)
- Docs: https://marpledata.gitlab.io/marple-sdk/
- Read through source code to understand types and available functions better
- Only make raw API calls using `DB.get` / `DB.post` / `Insight.post` (etc.) if no built-in function is available

## MATLAB SDK
- Installation: add the `matlab/` folder from https://github.com/marpledata/marple-sdk/tree/main/matlab to the MATLAB path (or download `DB.m` + `config.json`)
- Docs: https://docs.marpledata.com/docs/sdk/overview/matlab-sdk
- Read through `DB.m` for available methods
- Requires `config.json` next to `DB.m` (`api_url`, `api_key`, `workspace`, optional `datapool`)

## REST API
- Prefer Python or MATLAB SDK built-in functions because they are typed
- If not possible, prefer to call `DB.get(...)`, `Insight.post(...)` etc. (Python) or `DB.make_request(...)` (MATLAB) over raw HTTP calls
- Authenticate using `Authorization: Bearer <api-token>` for raw calls
- Swagger docs (DB): https://db.marpledata.com/api/docs
- Swagger docs (Insight): https://insight.marpledata.com/api/v1/spec/

## URLs
- Marple products exist in 3 deployment options: SaaS, VPC, Self-hosted
- By default, SDKs assume SaaS URLs (e.g. https://db.marpledata.com/api/v1/)
- If an API token returns 403, ask the user if they are on a different deployment than SaaS
```

## SQL via Trino

For server-side filtering/aggregation or BI tools, query Marple DB with SQL instead of pulling parquet: `db.query(sql) -> DataFrame` or `db.connect_trino()` for a raw connection. Not available on Marple SaaS yet. Connection details are auto-discovered; `db.trino_info` exposes the catalog names. Structure:

- Hot catalog (`<hot_catalog>`, Postgres metadata), schema `public`: `mdb_<datapool>_dataset`, `mdb_<datapool>_signal`, `mdb_<datapool>_signal_enum` (`name` -> `id`).
- Cold catalog (`<cold_catalog>`, Iceberg raw data), schema `<datapool>`: table `data` with columns `dataset`, `signal`, `time`, `value`, `value_text`. Keyed by dataset/signal IDs, so join `signal_enum` to filter by name.

Tables must be fully qualified, and hot + cold can be joined in one query. Full docs and examples: https://docs.marpledata.com/docs/marple-db/querying

## Marple Insight (secondary)

Most SDK work targets Marple DB. For Insight, use the `marple.Insight` client (`from marple import Insight`) or the REST API with `Authorization: Bearer <api-token>` and the Swagger reference: https://insight.marpledata.com/api/v1/spec/
