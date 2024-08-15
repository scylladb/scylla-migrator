===============
Getting Started
===============

Since the Migrator is packaged as a Spark application, you first have to set up a Spark cluster to use it. Then, submit the application along with its :doc:`configuration </configuration>` on the Spark cluster, which will execute the migration by reading from the source database and writing to the target database.

----------------------
Set Up a Spark Cluster
----------------------

A Spark cluster is made of several *nodes*, which can contain several *workers* (although there is usually just one worker per node). When you start the Migrator, the Spark *driver* looks at the job content and splits it into tasks. It then spawns *executors* on the cluster workers and feeds them with the tasks to compute.

We recommend provisioning at least 2 GB of memory per CPU on each node. For instance, a cluster node with 4 CPUs should have at least 8 GB of memory.

.. caution::

  Make sure the Spark version, the Scala version, and the Migrator version you use are `compatible together <../#compatibility-matrix>`_.

The following pages describe various alternative ways to set up a Spark cluster:

* :doc:`on your infrastructure, using Ansible </getting-started/ansible>`,
* :doc:`on your infrastructure, manually </getting-started/spark-standalone>`,
* :doc:`using AWS EMR </getting-started/aws-emr>`,
* or, :doc:`on a single machine, using Docker </getting-started/docker>`.

-----------------------
Configure the Migration
-----------------------

Once you have a Spark cluster ready to run the ``scylla-migrator-assembly.jar``, download the file `config.yaml.example <https://github.com/scylladb/scylla-migrator/blob/master/config.yaml.example>`_ and rename it to ``config.yaml``. This file contains properties such as ``source`` or ``target`` defining how to connect to the source database and to the target database, as well as other settings to perform the migration. Adapt it to your case according to the following guides:

- :doc:`Migrate from Apache Cassandra or Parquet files to ScyllaDB </migrate-from-cassandra-or-parquet>`.
- Or, :doc:`migrate from DynamoDB to ScyllaDB’s Alternator </migrate-from-dynamodb>`.

-----------------
Run the Migration
-----------------

Start the migration by invoking the ``spark-submit`` command with the appropriate arguments, as explained in the page :doc:`/run-the-migration`.

--------------
Extra Features
--------------

You might also be interested in the following extra features:

* :doc:`rename columns along the migration </rename-columns>`,
* :doc:`replicate changes applied to the source table after the initial snapshot transfer has completed </stream-changes>`,
* :doc:`validate that the migration was complete and correct </validate>`.

.. toctree::
    :hidden:

    ansible
    spark-standalone
    aws-emr
    docker
