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

Note that for the migration to be performed without loosing writes, the initial snapshot transfer must complete within 24 hours. Otherwise, some captured changes may be lost due to the retention period of the table’s stream.

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
     - ``1000``
     - Maximum records to fetch per ``GetRecords`` call (DynamoDB default).
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
