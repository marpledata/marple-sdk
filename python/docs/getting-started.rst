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

   from marple import Insight

   insight_token = "<your api token>"
   insight_url = "https://insight.marpledata.com/api/v1"

   insight = Insight(insight_token, insight_url)
   insight.check_connection()

   # For more advanced dataset/signal searches/exports, use DB instead of Insight.
   datasets = insight.get_datasets()
   dataset = datasets[0]
   signals = insight.get_signals(dataset["dataset_filter"])
