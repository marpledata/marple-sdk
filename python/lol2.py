# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marpledata",
#     "matplotlib",
#     "numpy",
# ]
#
# [tool.uv.sources]
# marpledata = { path = "." }
# ///

import marple
import random
import tempfile
import time
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

API_URL = "https://db.nightly.marpledata.com/api/v1"
API_TOKEN = "mdb_8Te7uxqnrxLbYqpLtGKySY6yPtMvu25vbNUaGsWvRhM"
# API_URL = "https://db.bugatti-rimac.marpledata.com"
# API_TOKEN = "mdb_2MlqjOz9zoJhNaOchaHSQgwhopsZipwoPMVONo_VE-k"


def reimport(db: marple.DB):
    stream = db.get_stream("January")
    dslist = stream.get_datasets().where_imported()
    print(dslist)
    print(dslist.to_pandas().head())

    key = time.time_ns()
    with tempfile.TemporaryDirectory() as temp_dir:
        for dataset in random.sample(dslist, 2):
            local_path = dataset.download(temp_dir)
            ds = stream.push_file(local_path, metadata={"source": "reimport", "key": key}, file_name=dataset.path)
            print(ds.id, ds.import_status)
    for dataset in stream.get_datasets().where_metadata({"key": key}).wait_for_import():
        print(dataset.id, dataset.import_status)
        dataset.delete()


def heatmap(db: marple.DB):
    datasets = db.get_stream(3).get_datasets() + db.get_stream(4).get_datasets()
    dfs = []
    for dataset, df in datasets.get_data(["EMS_EngineTorque", "EMS_EngineSpeed"], resample_rule="1s"):
        dfs.append(df)
    df = pd.concat(dfs).dropna(subset=["EMS_EngineTorque", "EMS_EngineSpeed"])
    fig, ax = plt.subplots()
    h = ax.hist2d(df["EMS_EngineTorque"], df["EMS_EngineSpeed"], bins=50, cmap="hot")
    fig.colorbar(h[3], ax=ax, label="Count")
    ax.set_xlabel("EMS_EngineTorque")
    ax.set_ylabel("EMS_EngineSpeed")
    ax.set_title("Engine Torque vs Speed Heatmap")
    plt.tight_layout()
    plt.show()


def show_datasets(db: marple.DB):
    dataset_ids = {
        "Original": 10802,
        "Optimized Row Groups": 576,
        "ZSTD Compression": 10814,
    }
    datasets = [db.get_dataset(id) for id in dataset_ids.values()]
    df = pd.DataFrame(
        [
            {
                "Test": label,
                "Million datapoints": int((d.n_datapoints or 0) // 1e6),
                "GiB raw": round((d.backup_size or 0) / (1024**3), 1),
                "GiB cold": round((d.cold_bytes or 0) / (1024**3), 1),
                "Import time (mins)": round((d.import_time or 0) / 60, 1),
            }
            for label, d in zip(dataset_ids.keys(), datasets)
        ]
    )
    print(df.to_string(index=False))


if __name__ == "__main__":
    db = marple.DB(API_TOKEN, API_URL)
    db.check_connection()
    dataset = random.choice(db.get_stream("Metrics").get_datasets())
    signal = random.choice(dataset.get_signals())
    print(signal.id, signal.name)
    print(signal.get_data())
    print(signal.download())
    print(signal.get_parquet_files())
    print(signal.get_data())
