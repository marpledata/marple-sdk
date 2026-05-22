Tutorials
=========

Import a file and wait for import
---------------------------------

.. code-block:: python

   from marple import DB

   stream_name = "Car data"
   api_token = "<your api token>"

   db = DB(api_token)
   stream = db.get_stream(stream_name)
   dataset = stream.push_file("examples_race.csv", metadata={"driver": "Mbaerto"})
   dataset = dataset.wait_for_import(timeout=10)

Upload large files
------------------

By default, ``stream.push_file(...)`` asks the Marple DB API how the file should
be uploaded. The API can select a server upload, direct Azure Blob Storage
upload, single presigned upload, or multipart upload.

For large files, increase ``concurrency`` to upload multiple parts in parallel
and use a longer import timeout:

.. code-block:: python

   from marple import DB

   stream_name = "Car data"
   api_token = "<your api token>"

   db = DB(api_token)
   stream = db.get_stream(stream_name)

   dataset = stream.push_file(
       "large_export.csv",
       metadata={"source": "testbench"},
       concurrency=8,
   )
   dataset = dataset.wait_for_import(timeout=180)

If your environment cannot reach direct storage URLs, force the upload through
the Marple DB API server:

.. code-block:: python

   dataset = stream.push_file(
       "large_export.csv",
       upload_mode="server",
   )
   dataset = dataset.wait_for_import(timeout=180)

Filter datasets and get resampled data
--------------------------------------

.. code-block:: python

   import re
   from marple import DB

   stream_name = "Car data"
   api_token = "<your api token>"

   db = DB(api_token)
   stream = db.get_stream(stream_name)

   datasets = stream.get_datasets()
   datasets = datasets.where_metadata({"car_id": [1, 2], "track": "track_1"})
   datasets = datasets.wait_for_import().where_imported()
   datasets = datasets.where_signal("car.speed", "max", greater_than=75)

   for dataset, data in datasets.get_data(
       signals=[
           "car.speed",
           re.compile("car.wheel.*.speed"),
           re.compile("car.wheel.*.trq"),
       ],
       resample_rule="0.17s",
   ):
       model.train(data)
