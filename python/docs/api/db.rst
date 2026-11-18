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
   marple.db.AliasingRule
   marple.db.DataStream
   marple.db.Dataset
   marple.db.DatasetList
   marple.db.DatasetRef
   marple.db.HeatBucket
   marple.db.LifecycleRules
   marple.db.MetadataField
   marple.db.SandboxJob
   marple.db.SandboxJobStatus
   marple.db.Script
   marple.db.ScriptVersion
   marple.db.Signal
   marple.db.SignalOccurrence
   marple.db.SignalPriorityImportResult
   marple.db.SignalPriorityResult
   marple.db.SignalSort
   marple.db.SignalStats
   marple.db.SignalStatsDetail
   marple.db.SignalStatsList
   marple.db.SignalStatsSummary
   marple.db.SignalStreamStats
   marple.db.SignalUpload
   marple.db.SignalsAlreadyExistError
   marple.db.StorageStatus
   marple.db.SCHEMA
   marple.db.LAKE_ARROW_SCHEMA
