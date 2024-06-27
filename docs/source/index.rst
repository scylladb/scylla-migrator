===============================
ScyllaDB Migrator Documentation
===============================

The Scylla Migrator is a Spark application that migrates data to ScyllaDB. Its main features are the following:

* it can read from Cassandra, Parquet, DynamoDB, or a DynamoDB S3 export,
* it can be distributed over multiple nodes of a Spark cluster to scale with your database cluster,
* it can rename columns along the way,
* it can transfer a snapshot of the source data, or continuously migrate new data as they come.

Read over the :doc:`Getting Started </getting-started/index>` page to set up a Spark cluster for a migration.

.. toctree::
  :hidden:

  getting-started/index
  migrate-from-cassandra-or-parquet
  migrate-from-dynamodb
  stream-changes
  rename-columns
  validate
  configuration
