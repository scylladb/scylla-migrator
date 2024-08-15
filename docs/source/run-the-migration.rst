=================
Run the Migration
=================

After you have `set up a Spark cluster <./getting-started#set-up-a-spark-cluster>`_ and `configured the migration <./getting-started#configure-the-migration>`_ you can start the migration by submitting a job to your Spark cluster. The command to use to submit the job depends on how the Spark cluster was installed. Please refer to the page that describes your Spark cluster setup to see how to invoke the ``spark-submit`` command. This page describes the arguments you need to pass to the ``spark-submit`` command to control the resources allocated to the migration job.

-----------------------
Invoke ``spark-submit``
-----------------------

The ``spark-submit`` command submits a job to the Spark cluster. You can run it from the Spark master node. You should supply the following arguments:

.. code-block:: bash

  spark-submit \
    --class com.scylladb.migrator.Migrator \
    --master spark://<spark-master-hostname>:7077 \
    --conf spark.scylla.config=<path to config.yaml> \
    --executor-cores 2 \
    --executor-memory 4G \
    <path to scylla-migrator-assembly.jar>

Here is an explanation of the arguments shown above:

- ``--class com.scylladb.migrator.Migrator`` sets the entry point of the Migrator.
- ``--master spark://<spark-master-hostname>:7077`` indicates the URI of the Spark master node. Replace ``<spark-master-hostname>`` with the actual hostname of your master node.
- ``--conf spark.scylla.config=<path to config.yaml>`` indicates the location of the migration :doc:`configuration file </configuration>`. It must be a path on the Spark master node.
- ``--executor-cores 2`` and ``--executor-memory 4G`` set the CPU and memory requirements for the Spark executors. See the section `below <#executor-resources>`_ for an explanation of how to set these values.
- Finally, ``<path to scylla-migrator-assembly.jar>`` indicates the location of the program binaries. It must be a path on the Spark master node.

------------------
Executor Resources
------------------

When the Spark master node starts the application, it breaks down the work into multiple tasks, and spawns *executors* on the worker nodes to compute these tasks.

.. caution:: You should explicitly indicate the CPU and memory requirements of the Spark executors, otherwise by default Spark will create a single executor using all the cores but only 1 GB of memory, which may not be enough and would lead to run-time errors such as ``OutOfMemoryError``.

The number of CPUs and the amount of memory to allocate to the Spark executors depends on the number of CPUs and amount of memory of the Spark worker nodes.

We recommend using between 5 to 10 CPUs per Spark executor. For instance, if your Spark worker node has 16 CPUs, you could use 8 CPUs per executor (the Spark driver would then allocate two executors on the worker to fully utilize its resources). You can control the number of CPUs per executors with the argument ``--executor-cores`` passed to the ``spark-submit`` command:

.. code-block:: bash

  --executor-cores 8

We also recommend using 2 GB of memory per CPU. So, if you provide 8 CPU per executor, you should require 16 GB of memory on the executor. You can control the amount of memory per executor with the argument ``--executor-memory`` passed to the ``spark-submit`` command:

.. code-block:: bash

  --executor-memory 16G

As long as your source and target databases are not saturated during the migration, you can increase the migration throughput by adding more worker nodes to your Spark cluster.

.. caution::

  To decrease the migration throughput, do not decrease the number of executor cores. Indeed, if you do that, Spark will simply allocate several executors to fully utilize the resources of the cluster. If you want to decrease the migration throughput, you can:

  - use a “smaller” Spark cluster (ie, with fewer worker nodes, each having fewer cores),
  - limit the number of total cores allocated to the application by passing the argument ``--conf spark.cores.max=2``,
  - in the case of a DynamoDB migration, decrease the value of the configuration properties ``throughputReadPercent`` and ``throughputWritePercent``.
