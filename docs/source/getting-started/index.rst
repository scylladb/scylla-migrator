===============
Getting Started
===============

Since the Migrator is packaged as a Spark application, you have to set up a Spark cluster to use it. Then, you submit the application along with its :doc:`configuration </configuration>` on the Spark cluster, which will execute the migration by reading from your source database and writing to your target database.

----------------------
Set Up a Spark Cluster
----------------------

The following pages describe various alternative ways to set up a Spark cluster:

* on your infrastructure, using :doc:`Ansible </getting-started/ansible>`,
* on your infrastructure, :doc:`manually </getting-started/spark-standalone>`,
* using :doc:`AWS EMR </getting-started/aws-emr>`,
* or, on a single machine, using :doc:`Docker </getting-started/docker>`.

-----------------------
Configure the Migration
-----------------------

Once you have a Spark cluster ready to run the ``scylla-migrator-assembly.jar``, download the file `config.yaml.example <https://github.com/scylladb/scylla-migrator/blob/master/config.yaml.example>`_ and rename it to ``config.yaml``. This file contains properties such as ``source`` or ``target`` defining how to connect to the source database and to the target database, as well as other settings to perform the migration. Adapt it to your case according to the following guides:

- :doc:`migrate from Cassandra or Parquet files to ScyllaDB </migrate-from-cassandra-or-parquet>`,
- or, :doc:`migrate from DynamoDB to ScyllaDB’s Alternator </migrate-from-dynamodb>`.

--------------
Extra Features
--------------

You might also be interested in the following extra features:

* :doc:`rename columns along the migration </rename-columns>`,
* :doc:`replicate changes applied to the source data after the initial snapshot transfer has completed </stream-changes>`,
* :doc:`validate that the migration was complete and correct </validate>`.

.. toctree::
    :hidden:

    ansible
    spark-standalone
    aws-emr
    docker
