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
