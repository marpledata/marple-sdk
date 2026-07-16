Marple DB
=========

Use ``DataStream.push_file`` for new file upload code. It exposes the current
upload controls, including ``concurrency`` for large direct uploads and
``upload_mode="server"`` for environments where direct storage URLs are blocked.
See :doc:`../tutorials` for upload examples.

.. autosummary::
   :toctree: .
   :nosignatures:

   marple.db.DB
   marple.db.DataStream
   marple.db.Dataset
   marple.db.DatasetList
   marple.db.Signal
