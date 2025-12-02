=================================================
Resume an Interrupted Migration Where it Left Off
=================================================

.. note:: This feature is currently supported only when migrating from Apache Cassandra or DynamoDB.

If, for some reason, the migration is interrupted (e.g., because of a networking issue, or if you need to manually stop it for some reason), the migrator is able to resume it from a “savepoints”.

Savepoints are configuration files that contain information about the already migrated items, which can be skipped when the migration is resumed. The savepoint files are automatically generated during the migration. To use a savepoint, start a migration using it as configuration file.

You can control the savepoints location and the interval at which they are generated in the configuration file under the top-level property ``savepoints``. See `the corresponding section of the configuration reference </configuration#savepoints>`__.

During the migration, the savepoints are generated with file names like ``savepoint_xxx.yaml``, where ``xxx`` is a timestamp looking like ``1234567890``. To resume a migration, start a new migration with the latest savepoint as configuration file.
