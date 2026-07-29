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
   ).wait_for_import(timeout=180)

Add signals to a dataset
------------------------

Add signals after import, or start with an empty dataset using
``stream.add_dataset``. Input follows :data:`marple.db.LAKE_ARROW_SCHEMA`:
``time`` (int64 nanoseconds) plus ``value`` and/or ``value_text``.

.. code-block:: python

   speed = dataset.get_signal("car.speed")
   assert speed is not None
   speed_data = speed.get_data()
   derived = pd.DataFrame({
       "time": speed_data.index.asi8,
       "value": speed_data["value"] * 3.6,
   })
   signal = dataset.add_signal(
       "car.speed_kmh",
       derived,
       metadata={"unit": "km/h"},
   ).wait_until_available()

For batches, ``add_signals`` returns signal IDs without waiting:

.. code-block:: python

   ids = dataset.add_signals(
       [{"name": "car.speed_kmh", "data": derived}],
       overwrite=True,
   )
   signals = [
       signal.wait_until_available()
       for signal in dataset.get_signals(signal_ids=ids, refresh=True)
   ]

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
