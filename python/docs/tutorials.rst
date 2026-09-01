Tutorials
=========

These examples use the high-level ``DataStream``, ``Dataset``, and ``Signal``
APIs. Create a stream and API token in Marple DB first.

Setup
-----

.. code-block:: python

   import os
   import re

   import pandas as pd

   from marple import DB

   db = DB(os.environ["MDB_TOKEN"])
   stream = db.get_stream("Car data")

For VPC or self-hosted deployments, pass ``os.environ["MDB_URL"]`` as the
second argument to ``DB``.

Import a file and wait for import
---------------------------------

.. code-block:: python

   dataset = stream.push_file(
       "examples_race.csv",
       metadata={"source": "testbench"},
       concurrency=8,
       overwrite=False,
   ).wait_for_import(timeout=180)

Pass ``overwrite=True`` to replace an existing dataset with the same name.

Add signals to a dataset
------------------------

Add signals after import, or start with an empty dataset using
``stream.add_dataset``. Input follows :data:`marple.db.LAKE_ARROW_SCHEMA`:
``time`` (int64 nanoseconds) plus ``value`` and/or ``value_text``.

A Series, or a DataFrame without a ``time`` column, takes its times from a
``DatetimeIndex`` or ``TimedeltaIndex``.

.. code-block:: python

   speed = dataset.get_signal("car.speed").get_data()
   signal = dataset.add_signal(
       "car.speed_kmh",
       speed * 3.6,
       metadata={"unit": "km/h"},
   ).wait_until_available()

Data assembled from scratch uses the explicit schema columns instead:

.. code-block:: python

   derived = pd.DataFrame({
       "time": [t0, t0 + 1_000_000_000],
       "value": [1.0, 2.0],
   })
   dataset.add_signal("car.custom", derived)

For batches, ``add_signals`` returns signal IDs without waiting:

.. code-block:: python

   ids = dataset.add_signals(
       [{"name": "car.speed_kmh", "data": speed * 3.6}],
       overwrite=True,
   )
   signals = [
       signal.wait_until_available()
       for signal in dataset.get_signals(signal_ids=ids, refresh=True)
   ]

Processing scripts
------------------

Write a ``process(dataset)`` function and try it on any imported dataset.
This runs in your Python process and writes to that dataset.

.. code-block:: python

   source = """
from marple.db import Dataset

def process(dataset: Dataset) -> None:
    speed = dataset.get_signal("car.speed").get_data()
    dataset.add_signal("car.speed_kmh", speed * 3.6, metadata={"unit": "km/h"})
"""

   dataset = stream.get_dataset(path="lap.csv")
   dataset.run_locally(source)

Alternatively, pass the path to a file that defines the ``process(dataset)`` function.

When the script looks right, store it and attach it to the stream. New uploads then run it after ingest.

.. code-block:: python

   script = db.create_script("speed_kmh", source, streams=[stream.id])

For files already imported, rerun aliasing and scripts, or start over from
the original file:

.. code-block:: python

   dataset = dataset.rerun_processing().wait_for_import()
   # or: dataset = dataset.reingest().wait_for_import()

Filter datasets and get resampled data
--------------------------------------

.. code-block:: python

   datasets = (
       stream.get_datasets()
       .where_metadata({"car_id": [1, 2], "track": "track_1"})
       .wait_for_import()
       .where_imported()
       .where_signal("car.speed", "max", greater_than=75)
   )
   for dataset, data in datasets.get_data(
       signals=["car.speed", re.compile(r"car\.wheel\..*\.speed")],
       resample_rule="0.17s",
   ):
       print(dataset.path, data.shape)

Ingest realtime data
--------------------

Use ``append`` with a realtime stream, then ``cool`` the dataset to cold
storage when ingestion is complete.

.. code-block:: python

   realtime = db.get_stream("Live car data")
   dataset = realtime.add_dataset("race-1", metadata={"driver": "Alice"})
   dataset.upsert_signals([{"signal": "car.speed", "unit": "m/s"}])
   dataset.append(pd.DataFrame({
       "time": [1_700_000_000_000_000_000, 1_700_000_001_000_000_000],
       "car.speed": [10.0, 12.0],
   }))
   dataset = dataset.cool().wait_for_import(timeout=180)

Query with SQL
--------------

Trino querying is available on VPC and self-hosted deployments, not SaaS.

.. code-block:: python

   info = db.trino_info
   table = f"{info['cold_catalog']}.{info['datapool']}.data"
   data = db.query(
       f"SELECT time, signal, value FROM {table} WHERE dataset = ? LIMIT 1000",
       params=[dataset.id],
   )
