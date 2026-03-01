==============
Stream Changes
==============

Instead of terminating immediately after having copied a snapshot of the source table, the migrator can also keep running and endlessly replicate the changes applied to the source table as they arrive. This feature is only supported when :doc:`reading from DynamoDB and writing to ScyllaDB Alternator </migrate-from-dynamodb>`.

It works by enabling, on the source table, a `DynamoDB Stream <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html>`_ that emits the changed items data each time an application creates, updates, or deletes data in the source table.

Enable this feature by setting the property ``streamChanges`` to ``true`` in the target database configuration:

.. code-block:: yaml

  target:
    type: dynamodb
    # ...
    # ... Full configuration not repeated here for the sake of brevity
    # ...
    # Enable the feature
    streamChanges: true

In this mode, the migrator has to be interrupted manually with ``Control`` + ``C`` (or by sending a ``SIGINT`` signal to the ``spark-submit`` process). Currently, the created stream is not deleted when the migrator is stopped. You have to delete it manually (e.g. via the AWS Console).

Note that for the migration to be performed without losing writes, the initial snapshot transfer must complete within 24 hours. Otherwise, some captured changes may be lost due to the retention period of the table’s stream.

Optionally, you can skip the initial snapshot transfer and only replicate the changed items by setting the property ``skipInitialSnapshotTransfer`` to ``true``:

.. code-block:: yaml

  target:
    type: dynamodb
    # ...
    streamChanges: true
    skipInitialSnapshotTransfer: true

Tuning Stream Replication
-------------------------

The following optional properties can be set in the **source** configuration to tune stream replication behavior:

.. list-table::
   :header-rows: 1
   :widths: 35 15 50

   * - Property
     - Default
     - Description
   * - ``streamingPollIntervalSeconds``
     - ``5``
     - How often (in seconds) to poll DynamoDB Streams for new records.
   * - ``streamingMaxConsecutiveErrors``
     - ``50``
     - Maximum consecutive poll failures before stopping stream replication.
   * - ``streamingPollingPoolSize``
     - ``max(4, CPUs)``
     - Thread pool size for polling shards in parallel.
   * - ``streamingLeaseDurationMs``
     - ``60000``
     - Lease duration in milliseconds. If a worker doesn't renew within this window, other workers can claim the shard.
   * - ``streamingMaxRecordsPerPoll``
     - unset (DynamoDB service default: ``1000``)
     - Maximum records to fetch per ``GetRecords`` call. When not set, the DynamoDB Streams service default of 1000 is used.
   * - ``streamingMaxRecordsPerSecond``
     - unlimited
     - Maximum records processed per second across all shards. Use this to avoid overwhelming the target.
   * - ``streamingEnableCloudWatchMetrics``
     - ``false``
     - Publish stream replication metrics (records processed, active shards, iterator age) to CloudWatch.
   * - ``streamApiCallTimeoutSeconds``
     - ``30``
     - Overall timeout for DynamoDB Streams API calls (seconds).
   * - ``streamApiCallAttemptTimeoutSeconds``
     - ``10``
     - Per-attempt timeout for DynamoDB Streams API calls (seconds).
   * - ``streamingPollFutureTimeoutSeconds``
     - ``60``
     - Timeout for awaiting parallel shard poll results. Increase if the polling pool is saturated and polls take longer than expected.

Example:

.. code-block:: yaml

  source:
    type: dynamodb
    table: my-table
    # ...
    streamingPollIntervalSeconds: 2
    streamingMaxRecordsPerSecond: 5000
    streamingPollingPoolSize: 8

Checkpoint Table
----------------

The migrator creates a DynamoDB table named ``migrator_<tableName>_<hash>`` in the source account to track replication progress. This table stores the last processed sequence number for each shard and is used for lease coordination when multiple workers are running.

The checkpoint table has a single hash key (``leaseKey``) and the following columns:

.. list-table::
   :header-rows: 1
   :widths: 25 10 65

   * - Column
     - Type
     - Description
   * - ``leaseKey``
     - S
     - Hash key. The DynamoDB Streams shard ID.
   * - ``checkpoint``
     - S
     - Last successfully processed sequence number, or ``SHARD_END`` when the shard is fully consumed.
   * - ``leaseOwner``
     - S
     - Worker ID that currently owns this shard's lease.
   * - ``leaseExpiryEpochMs``
     - N
     - Epoch milliseconds when the lease expires. Set to ``0`` when released.
   * - ``parentShardId``
     - S
     - Parent shard ID (if any), used to ensure child shards are processed after their parent is drained.
   * - ``leaseTransferTo``
     - S
     - Worker ID requesting a graceful lease transfer. Removed when the lease is released.
   * - ``leaseCounter``
     - N
     - Monotonically increasing counter incremented on each lease renewal.

The checkpoint table persists across restarts. If the migrator is restarted, it automatically resumes from the last checkpointed position rather than re-processing the entire stream from the beginning. To force re-processing from ``TRIM_HORIZON``, manually delete the checkpoint table before restarting the migrator.

Reserved Attribute Names
------------------------

CloudWatch Metrics
------------------

When ``streamingEnableCloudWatchMetrics`` is enabled, the migrator publishes the following metrics to the ``ScyllaMigrator/StreamReplication`` namespace. All metrics include a ``TableName`` dimension set to the source table name.

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Metric Name
     - Unit
     - Description
   * - ``RecordsProcessed``
     - Count
     - Number of records processed since the last publish interval (delta).
   * - ``ActiveShards``
     - Count
     - Number of shards currently owned by this worker.
   * - ``MaxIteratorAgeMs``
     - Milliseconds
     - Age of the oldest record seen in the most recent poll cycle, indicating how far behind the stream consumer is.
   * - ``PollDurationMs``
     - Milliseconds
     - Wall-clock time of the most recent poll cycle.
   * - ``WriteFailures``
     - Count
     - Number of write failures since the last publish interval (delta).
   * - ``CheckpointFailures``
     - Count
     - Number of checkpoint failures since the last publish interval (delta).
   * - ``DeadLetterItems``
     - Count
     - Number of items that exhausted write retries since the last publish interval (delta).

Metrics are published every 60 poll cycles (e.g., every 5 minutes with the default 5-second poll interval).

Reserved Attribute Names
------------------------

The stream replication process uses an internal attribute named ``_dynamo_op_type`` to distinguish between put and delete operations. If a source table has an attribute with this exact name, it will be silently overwritten during stream replication. Avoid using ``_dynamo_op_type`` as an attribute name in tables being migrated.
