# Dataset 8 fixtures

Canonical Marple DB lake Parquet files used by `prepare-upload` tests.

Directory names (`dataset=8`, `signal=82`, `signal=86`) are labels only. The
embedded Iceberg identity columns are:

| File | Embedded `dataset` | Embedded `signal` | Kind |
| --- | ---: | ---: | --- |
| `signal=86/mdb_Usage_kWh.parquet` | 5863 | 821456 | numeric (`value`) |
| `signal=82/mdb_Load_Type.parquet` | 5863 | 821465 | text (`value_text`) |

## Source attribution

Derived from the [Steel Industry Energy Consumption](https://archive.ics.uci.edu/dataset/851/steel%2Bindustry%2Benergy%2Bconsumption)
dataset (UCI Machine Learning Repository), licensed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

Creators: Sathishkumar V E, Changsun Shin, Yongyun Cho.
