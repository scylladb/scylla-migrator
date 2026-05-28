=================================================
Resume an Interrupted Migration Where it Left Off
=================================================

.. note:: Savepoints support Apache Cassandra/ScyllaDB CQL, DynamoDB/Alternator, and Parquet with file-level tracking. MySQL and DynamoDB S3-export sources are not resumable.

If, for some reason, the migration is interrupted (e.g., because of a networking issue, or if you need to manually stop it for some reason), the migrator is able to resume it from a “savepoint”.

Savepoints are configuration files that contain information about the already migrated items, which can be skipped when the migration is resumed. The savepoint files are automatically generated during the migration.

Automatic resume
================

By default, the migrator resumes automatically. On startup it scans the configured ``savepoints`` location, finds the most recent savepoint, and merges its progress (the already-migrated token ranges, scan segments, or Parquet files) into the configuration it was launched with. This means that **re-running the same migration with the same configuration file continues where the previous run left off** — no manual editing or file selection is required. This is especially convenient for orchestrated environments (Kubernetes, Cloud Run, etc.) that restart the job with a fixed configuration.

Only the progress information is taken from the savepoint; every other setting — in particular the source and target credentials — comes from the configuration you launch with. You can disable this behavior by setting ``savepoints.autoResume`` to ``false``, for example to force a full re-migration while reusing the same savepoints directory. See the :ref:`configuration reference <config-savepoints>`.

.. warning::
   Savepoint files are written with credentials **redacted** (secrets are replaced with ``<redacted>``). They are therefore safe to keep in shared or long-lived storage such as an S3/GCS bucket, but a savepoint file can no longer be used directly as a standalone configuration file. Use automatic resume (above) instead, which supplies the real credentials from your launch configuration.

You can control the savepoints location and the interval at which they are generated in the configuration file under the top-level property ``savepoints``. See the corresponding section of the :ref:`configuration reference <config-savepoints>`.

Savepoint file names
====================

File-backed savepoints are the default. With this backend, savepoints are generated as files in the configured directory.

During the migration, the savepoints are generated with file names like ``savepoint_<epochMillis>_<counter>.yaml``, where:

* ``<epochMillis>`` is a 13-digit, zero-padded millisecond timestamp (monotonic within a migrator instance), and
* ``<counter>`` is a 10-digit, zero-padded per-instance sequence number that disambiguates dumps produced within the same millisecond.

Both components are zero-padded so that **lexicographical order matches chronological order**: listing the directory and picking the alphabetically last file (``ls <savepoints-dir> | tail -n 1``) always yields the most recent savepoint, which is the one automatic resume selects.

.. note::
   Older versions of the migrator produced file names like ``savepoint_1234567890.yaml`` (a single Unix-epoch-seconds component). The migrator still reads those files, but any new savepoint written during a resumed migration follows the two-component format above.

Target-Database Savepoints
==========================

When the migration uses a CQL target (``target.type: scylla`` or its ``cassandra`` alias), savepoints can be stored in the target database instead of on the Spark driver filesystem:

.. code-block:: yaml

  savepoints:
    intervalSeconds: 300
    autoResume: true
    target:
      type: target-table
      # Optional. Default: target.keyspace.
      keyspace: null
      # Optional. Created automatically.
      table: scylla_migrator_savepoints
      # Optional stable job id override.
      jobId: null

An omitted ``savepoints.target.keyspace`` inherits the exact case of ``target.keyspace``. Explicit metadata keyspace and table names use CQL identifier rules: ``MyKs`` resolves to ``myks``, while the YAML value ``'"MyKs"'`` preserves case. See the :ref:`configuration reference <config-savepoints>` for a complete quoted-name example.

The metadata keyspace must already exist. The migrator creates the metadata table automatically and appends one row per savepoint version. Re-running the original configuration automatically resumes from the newest valid matching table savepoint. Set ``savepoints.autoResume: false`` to disable lookup.

If ``savepoints.target.jobId`` is omitted, the migrator derives a stable id from the redacted configuration identity, including source, target and renames, excluding ``skipTokenRanges``, ``skipSegments``, ``skipParquetFiles``, ``savepoints``, and ``validation``. Use the same original config to resume so the derived job id stays the same. An explicit ``savepoints.target.jobId`` selects the rows to inspect but does not bypass configuration identity checks: changes to identity settings such as source, target or renames prevent those rows from being resumed. During lookup, the migrator reads rows for that job from newest to oldest and skips corrupt or unparseable YAML rows with warnings.

.. warning::
   Target-database savepoints store redacted YAML. Resume restores only progress and keeps the launch credentials and other settings. Missing or failed lookups warn and continue; they can cause completed work to be repeated.
