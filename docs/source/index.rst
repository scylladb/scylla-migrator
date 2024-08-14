===============================
ScyllaDB Migrator Documentation
===============================

The ScyllaDB Migrator is a Spark application that migrates data to ScyllaDB. Its main features are the following:

* It can read from Apache Cassandra, Parquet, DynamoDB, or a DynamoDB S3 export.
* It can be distributed over multiple nodes of a Spark cluster to scale with your database cluster.
* It can rename columns along the way.
* When migrating from DynamoDB it can transfer a snapshot of the source data, or continuously migrate new data as they come.

Read over the :doc:`Getting Started </getting-started/index>` page to set up a Spark cluster and to configure your migration. Alternatively, follow our :doc:`step-by-step tutorial to perform a migration between fake databases using Docker </tutorials/dynamodb-to-scylladb-alternator/index>`.

--------------------
Compatibility Matrix
--------------------

The following table summarizes the required version of Spark and Scala for each release of the migrator. Please make sure to set up a Spark environment compatible with the version of the migrator that you are using.

========  =====  ======
Migrator  Spark  Scala
========  =====  ======
0.9.x     3.5.x  2.13.x
========  =====  ======

.. toctree::
  :hidden:

  getting-started/index
  migrate-from-cassandra-or-parquet
  migrate-from-dynamodb
  stream-changes
  rename-columns
  validate
  configuration
  tutorials/index
