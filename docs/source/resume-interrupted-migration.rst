=================================================
Resume an Interrupted Migration Where it Left Off
=================================================

.. note:: This feature is supported for resumable sources such as Apache Cassandra/ScyllaDB CQL, DynamoDB/Alternator, and Parquet with file-level tracking enabled. MySQL migrations are not resumable.

If, for some reason, the migration is interrupted (e.g., because of a networking issue, or if you need to manually stop it for some reason), the migrator is able to resume it from a “savepoints”.

Savepoints are configuration files that contain information about the already migrated items, which can be skipped when the migration is resumed. The savepoint files are automatically generated during the migration. To use a savepoint, start a migration using it as configuration file.

You can control the savepoints location and the interval at which they are generated in the configuration file under the top-level property ``savepoints``. See the corresponding section of the :ref:`configuration reference <config-savepoints>` .

File-backed savepoints are the default. With this backend, savepoints are generated as files in the configured directory.

During the migration, the savepoints are generated with file names like ``savepoint_<epochMillis>_<counter>.yaml``, where:

* ``<epochMillis>`` is a 13-digit, zero-padded millisecond timestamp (monotonic within a migrator instance), and
* ``<counter>`` is a 10-digit, zero-padded per-instance sequence number that disambiguates dumps produced within the same millisecond.

Both components are zero-padded so that **lexicographical order matches chronological order**: listing the directory and picking the alphabetically last file (``ls <savepoints-dir> | tail -n 1``) always yields the most recent savepoint. To resume a migration, start a new migration with that latest savepoint as configuration file.

.. note::
   Older versions of the migrator produced file names like ``savepoint_1234567890.yaml`` (a single Unix-epoch-seconds component). The migrator still reads those files, but any new savepoint written during a resumed migration follows the two-component format above.

Target-Database Savepoints
---------------------------

When the migration target is ScyllaDB (``target.type: scylla``), savepoints can be stored in the target database instead of on the Spark driver filesystem:

.. code-block:: yaml

  savepoints:
    intervalSeconds: 300
    resumeFromLatest: false
    target:
      type: target-table
      # Optional. Default: target.keyspace.
      keyspace: null
      # Optional. Created automatically.
      table: scylla_migrator_savepoints
      # Optional stable job id override.
      jobId: null

The metadata keyspace must already exist. The migrator creates the metadata table automatically and appends one row per savepoint version. To resume from the newest target-database savepoint, run the original configuration with:

.. code-block:: yaml

  savepoints:
    target:
      type: target-table
    resumeFromLatest: true

If ``target.jobId`` is omitted, the migrator derives a stable id from the redacted source and target configuration identity, excluding progress fields, ``savepoints``, and ``validation``. Use the same original config to resume so the derived job id stays the same. During lookup, the migrator reads rows for that job from newest to oldest and skips corrupt or unparseable YAML rows with warnings.

.. warning::
   Target-database savepoints store the same unredacted YAML payload as file-backed savepoints. The ``config_yaml`` column may contain credentials and other sensitive values. Restrict access to the metadata table.
