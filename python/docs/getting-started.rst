Getting Started
===============

Install
-------

Install the Marple SDK using your package manager:

.. code-block:: bash

   poetry add marpledata
   uv add marpledata
   pip install marpledata

The SDK exposes the main entry points:

.. code-block:: python

   from marple import DB
   from marple import Insight

Marple DB quickstart
--------------------

.. code-block:: python

   from marple import DB

   stream_name = "Car data"
   api_token = "<your api token>"
   api_url = "https://db.marpledata.com/api/v1"

   db = DB(api_token, api_url)
   db.check_connection()

   stream = db.get_stream(stream_name)
   dataset = stream.push_file("examples_race.csv", metadata={"driver": "Mbaerto"})
   dataset = dataset.wait_for_import(timeout=10)

Marple Insight quickstart
-------------------------

.. code-block:: python

   from marple import DB, Insight

   insight_token = "<your api token>"
   insight_url = "https://insight.marpledata.com/api/v1"
   db_token = "<your api token>"
   db_url = "https://db.marpledata.com/api/v1"
   stream_name = "Car data"

   insight = Insight(insight_token, insight_url)
   db = DB(db_token, db_url)

   dataset_id = db.get_datasets(stream_name)[0].id
   insight_dataset = insight.get_dataset_mdb(dataset_id)

   file_path = insight.export_data_mdb(
       dataset_id,
       format="h5",
       signals=["car.speed"],
       destination=".",
   )
   print("Wrote", file_path)
