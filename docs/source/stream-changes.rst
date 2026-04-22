==============
Stream Changes
==============

Instead of terminating immediately after having copied a snapshot of the source table, the migrator can also keep running and endlessly replicate the changes applied to the source table as they arrive. This feature is only supported when :doc:`reading from DynamoDB and writing to ScyllaDB Alternator </migrate-from-dynamodb>`.

Two change-capture modes are available:

* **DynamoDB Streams** (the default, enabled by ``streamChanges: true``). The migrator reads directly from the source table's DynamoDB Stream, which has a **24-hour retention**. If your initial snapshot transfer takes longer than 24 hours, writes that happened at the beginning of the window may be lost.
* **Kinesis Data Streams for DynamoDB**. The migrator reads from a pre-created Kinesis Data Stream that the source table emits change records into. Retention is configurable up to **one year**, and the migrator replays from an ``AT_TIMESTAMP`` captured immediately before the snapshot — so multi-day snapshots no longer drop writes.

DynamoDB Streams mode (legacy, 24h retention)
---------------------------------------------

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

Note that for the migration to be performed without losing writes, the initial snapshot transfer must complete within 24 hours. Otherwise, some captured changes may be lost due to the retention period of the table's stream. If your snapshot is expected to take longer than that, use the Kinesis Data Streams mode instead.

Optionally, you can skip the initial snapshot transfer and only replicate the changed items by setting the property ``skipInitialSnapshotTransfer`` to ``true``:

.. code-block:: yaml

  target:
    type: dynamodb
    # ...
    streamChanges: true
    skipInitialSnapshotTransfer: true

.. warning::

   Starting with this version, the KCL lease table name used for checkpointing is deterministic: ``migrator_<source-table>`` (previously: ``migrator_<source-table>_<millis>``).

   * **Upgrade impact**: if you were already running on DynamoDB Streams, your existing ``migrator_<source-table>_<millis>`` lease tables become orphaned in DynamoDB. They continue to bill silently; delete them manually after confirming the new lease table has taken over.
   * **Fresh replay**: on the first run after upgrade the migrator cannot find the prior ``_<millis>``-suffixed lease table and will replay the stream from ``TrimHorizon``. Apply this upgrade during a quiet window.
   * **Concurrent migrators**: if you run two migrators against the same source table (e.g. replicating to two different targets) they now share a lease table and KCL will split the shards between them — each target sees only half of the changes. The DynamoDB Streams path does not currently expose an ``appName`` override; until one is added, avoid concurrent migrators on a single source table, or use the Kinesis Data Streams mode (below) which supports ``appName``.

Kinesis Data Streams mode (up to 1y retention)
----------------------------------------------

For large source tables where the initial snapshot transfer cannot complete within the DynamoDB Streams 24-hour window, use the Kinesis Data Streams mode. This reads change records from a pre-existing Kinesis Data Stream that the source DynamoDB table publishes into — retention is tunable up to one year and replay can start from a specific timestamp.

**Prerequisites**

#. **Create the Kinesis Data Stream** yourself (the migrator does not create it). Decide shard count, retention (``StreamModeDetails`` / ``RetentionPeriodHours``), and KMS encryption based on your workload. A small table can start with one shard; a hot table should use more.
#. **IAM permissions** for the migrator's credentials. The migrator needs:

   .. code-block:: json

      {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "dynamodb:DescribeKinesisStreamingDestination",
              "dynamodb:EnableKinesisStreamingDestination",
              "dynamodb:DescribeTable",
              "kinesis:DescribeStream",
              "kinesis:DescribeStreamSummary",
              "kinesis:GetRecords",
              "kinesis:GetShardIterator",
              "kinesis:ListShards",
              "kinesis:ListStreams",
              "kinesis:SubscribeToShard"
            ],
            "Resource": "*"
          },
          {
            "Effect": "Allow",
            "Action": [
              "dynamodb:CreateTable",
              "dynamodb:DeleteItem",
              "dynamodb:DescribeTable",
              "dynamodb:GetItem",
              "dynamodb:PutItem",
              "dynamodb:Scan",
              "dynamodb:UpdateItem"
            ],
            "Resource": "arn:aws:dynamodb:*:*:table/migrator_*"
          },
          {
            "Effect": "Allow",
            "Action": [
              "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
          }
        ]
      }

   The second statement grants access to the KCL lease table (``migrator_<source-table>`` by default). The CloudWatch permission is required by KCL to report per-shard metrics.

**Configuration**

.. code-block:: yaml

  target:
    type: dynamodb
    # ...
    # ... Full configuration not repeated here for the sake of brevity
    # ...
      streamChanges:
        type: kinesis
        # Full ARN of a pre-created Kinesis Data Stream. Bare stream names are NOT accepted:
        # DynamoDB's EnableKinesisStreamingDestination API requires a full ARN. The ARN must
        # match the pattern
        #   arn:<partition>:kinesis:<region>:<12-digit-account>:stream/<name>
        # where <partition> is aws, aws-cn, or aws-us-gov. The ARN's region must also match
        # the source.region above — a mismatch is rejected at startup.
        streamArn: arn:aws:kinesis:us-east-1:123456789012:stream/my-dynamo-stream

      # Optional — ISO-8601 instant used as the KCL AT_TIMESTAMP initial position. When omitted,
      # the migrator captures `Instant.now()` on the driver JVM right after the Kinesis
      # streaming destination reaches ACTIVE, so changes that happen during the initial
      # snapshot transfer are replayed afterwards. NOTE: this default relies on the driver's
      # **local wall clock** — if the driver is not NTP-synced, AT_TIMESTAMP can miss or
      # duplicate records near the snapshot boundary. See the "Driver clock skew" warning
      # below. Set this field explicitly to resume from a known checkpoint or to bypass the
      # default clock-skew risk. An initialTimestamp more than 5 minutes in the future is
      # rejected at load time — it is almost always a typo or driver clock skew.
      # initialTimestamp: 2026-04-13T08:00:00Z

      # Optional — KCL application name (a.k.a. lease-table name in DynamoDB). Defaults to
      # `migrator_<source-table>_<arn-hash-8>` so two migrators that fan out the same source
      # table to two different Kinesis destinations each get their own lease table. Override
      # this ONLY when you have a pre-existing lease table you want to reuse (e.g. a restart
      # after a migrator-version upgrade). Two migrators sharing an appName share a lease table
      # and KCL splits shard ownership between them.
      # appName: migrator_my-dynamo-table_to_alternator_east

The ``streamChanges`` field accepts three shapes, in order of precedence:

* ``false`` — snapshot-only (no streaming). Same as omitting the field historically.
* ``true`` — DynamoDB Streams mode (the default streaming mode).
* An object with ``type: kinesis`` (or the alias ``kinesis-data-streams``) — Kinesis Data Streams mode.

Existing YAML files that use the boolean form continue to work unchanged.

**When to prefer Kinesis over DynamoDB Streams**

================================================================================================ ================================== =========================
Scenario                                                                                         DynamoDB Streams                   Kinesis Data Streams
================================================================================================ ================================== =========================
Snapshot finishes within 24h                                                                     ✓ Simpler, zero-config             ✓ Works too
Snapshot may exceed 24h (hundreds of GB, slow target)                                            ✗ Changes older than 24h are lost  ✓ Up to 1y retention
Want to fan-out the same source to multiple targets concurrently                                 ✗ No ``appName`` override yet      ✓ ``appName`` per target
Want ``AT_TIMESTAMP`` replay from a known moment                                                 ✗ Only ``TrimHorizon``             ✓ ``initialTimestamp``
Want the migrator to create and manage the stream for you                                        ✓ DDB Stream is created on enable  ✗ You pre-create the stream
================================================================================================ ================================== =========================

As in DynamoDB Streams mode, the migrator has to be interrupted manually. Deleting the Kinesis stream after the migration is the user's responsibility; the migrator does not disable the DynamoDB → Kinesis destination on shutdown so that a subsequent restart can resume without data loss.

.. warning::

   **Default AT_TIMESTAMP precision depends on driver clock sync.** When ``initialTimestamp`` is omitted (and no S3-export ``exportTime`` is available), the migrator captures ``Instant.now()`` on the driver JVM right after the Kinesis streaming destination reaches ACTIVE. Kinesis Data Streams compares this instant against ``ApproximateArrivalTimestamp``, which AWS stamps using its internal clock. If the driver's clock is skewed from the AWS clock by ``Δ``:

   * **Driver clock AHEAD by Δ** → KDS starts reading from the future → writes in the first ``Δ`` seconds are **silently LOST** (no error, no retry — the records simply never reach the target).
   * **Driver clock BEHIND by Δ** → KDS starts reading from the past → ``Δ`` seconds of events are replayed. DynamoDB ``PutItem`` is idempotent on the same primary key, so the cost is only a little extra write load on the target.

   The migrator does **not** apply a default safety margin to ``Instant.now()``. To avoid lost writes:

   #. Run the migrator driver on a host with **NTP sync**. AWS EC2 instances running Amazon Linux, the official Ubuntu AMIs, or any distribution with default ``chrony`` have sub-millisecond drift and need no action. Non-AWS hosts should run ``chronyd``, ``systemd-timesyncd``, or the platform equivalent before starting the migrator.
   #. For hosts with weak clock guarantees (laptops, fresh containers, recently-suspended or -resumed VMs), **set ``initialTimestamp`` explicitly** to an ISO-8601 instant slightly earlier than your migration start — the explicit value bypasses the ``Instant.now()`` default entirely. When in doubt, bias earlier: a few seconds of duplicates are harmless, a few seconds of lost writes are not recoverable without re-running from scratch.
   #. If you are chaining an S3 export with streaming (see below), the default ``AT_TIMESTAMP`` comes from the export's ``exportTime`` — an AWS-server-generated instant — and does **not** depend on the driver's clock.

Chaining S3-export with streamChanges
-------------------------------------

When the source is a DynamoDB S3 export (``type: dynamodb-s3-export``), the export itself is a point-in-time snapshot — there is no live stream attached to it. To keep the target in sync with writes that happen after the export was produced, supply a ``streamSource`` block alongside the export pointing at the **still-live** DynamoDB table the export was taken from:

.. code-block:: yaml

  source:
    type: dynamodb-s3-export
    bucket: my-export-bucket
    manifestKey: AWSDynamoDB/abc123/manifest-summary.json
    # ... regular S3 export options (region, credentials, tableDescription, ...)
    streamSource:
      type: dynamodb
      table: MyLiveTable
      region: us-east-1
      # ... same options you would use for a direct `source: dynamodb:` migration

  target:
    type: dynamodb
    # ... target config
    streamChanges:
      type: kinesis
      streamArn: arn:aws:kinesis:us-east-1:123456789012:stream/my-dynamo-stream
      # initialTimestamp is OPTIONAL — the migrator auto-defaults it to the export's
      # `exportTime` (or `startTime` as a fallback) from `manifest-summary.json`, so writes
      # that happened after the export was produced are replayed from that instant.

**Behaviour**

* The migrator applies the S3 export as the initial snapshot (no DynamoDB read load on the live table).
* Once the snapshot is applied, the migrator enables the configured streaming destination (``DynamoDB Streams`` or ``Kinesis Data Streams``) on the **live table** referenced by ``streamSource``.
* For the Kinesis path, the default ``AT_TIMESTAMP`` is the export's ``exportTime`` from ``manifest-summary.json``. This addresses `GitHub issue #250 <https://github.com/scylladb/scylla-migrator/issues/250>`_ acceptance criterion #4.
* If the manifest does not contain ``exportTime`` or ``startTime`` (e.g. a very old export format), the migrator logs a warning and falls back to ``Instant.now()`` — writes between the export and that instant are lost. Set ``streamChanges.initialTimestamp`` explicitly in that case.
* If you omit ``streamSource`` while requesting ``streamChanges``, the migrator **fails fast** at startup rather than silently skipping the streaming phase (finding ARCH-1 from the cross-model review).

