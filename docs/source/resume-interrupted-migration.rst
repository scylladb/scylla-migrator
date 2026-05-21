=================================================
Resume an Interrupted Migration Where it Left Off
=================================================

.. note:: This feature is currently supported only when migrating from Apache Cassandra or DynamoDB.

If, for some reason, the migration is interrupted (e.g., because of a networking issue, or if you need to manually stop it for some reason), the migrator is able to resume it from a “savepoints”.

Savepoints are configuration files that contain information about the already migrated items, which can be skipped when the migration is resumed. The savepoint files are automatically generated during the migration. To use a savepoint, start a migration using it as configuration file.

You can control the savepoints location and the interval at which they are generated in the configuration file under the top-level property ``savepoints``. See the corresponding section of the :ref:`configuration reference <config-savepoints>` .

During the migration, the savepoints are generated with file names like ``savepoint_<epochMillis>_<counter>.yaml``, where:

* ``<epochMillis>`` is a 13-digit, zero-padded millisecond timestamp (monotonic within a migrator instance), and
* ``<counter>`` is a 10-digit, zero-padded per-instance sequence number that disambiguates dumps produced within the same millisecond.

Both components are zero-padded so that **lexicographical order matches chronological order**: listing the directory and picking the alphabetically last file (``ls <savepoints-dir> | tail -n 1``) always yields the most recent savepoint. To resume a migration, start a new migration with that latest savepoint as configuration file.

.. note::
   Older versions of the migrator produced file names like ``savepoint_1234567890.yaml`` (a single Unix-epoch-seconds component). The migrator still reads those files, but any new savepoint written during a resumed migration follows the two-component format above.
