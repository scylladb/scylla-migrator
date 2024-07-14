======================
Validate the Migration
======================

In addition to the monitoring user interface provided by Spark, you can run another program, after running a migration, to check whether the destination table contains exactly the same items as the source table.

Running this program consists of submitting the same Spark job as the migration, but with a different entry point.

Before running the validator, adjust the corresponding configuration in the top-level ``validation`` property of the file ``config.yml``:

.. code-block:: yaml

  validation:
    # Should WRITETIMEs and TTLs be compared?
    compareTimestamps: true
    # What difference should we allow between TTLs?
    ttlToleranceMillis: 60000
    # What difference should we allow between WRITETIMEs?
    writetimeToleranceMillis: 1000
    # How many differences to fetch and print
    failuresToFetch: 100
    # What difference should we allow between floating point numbers?
    floatingPointTolerance: 0.001
    # What difference in ms should we allow between timestamps?
    timestampMsTolerance: 0

The exact way to run the validator depends on the way you set up the Spark cluster.

--------------------------------------------
Run the Validator in the Ansible-based Setup
--------------------------------------------

Submit the job ``submit-cql-job-validator.sh``.

-----------------------------------------
Run the Validator in a Manual Spark Setup
-----------------------------------------

Pass the argument ``--class com.scylladb.migrator.Validator`` to the ``spark-submit`` invocation:

.. code-block:: bash

  spark-submit --class com.scylladb.migrator.Validator \
    --master spark://<spark-master-hostname>:7077 \
    --conf spark.scylla.config=<path to config.yaml> \
    <path to scylla-migrator-assembly.jar>

----------------------------
Run the Validator in AWS EMR
----------------------------

Use the following arguments for the Cluster Step that runs a custom JAR:

.. code-block:: text

  spark-submit --deploy-mode cluster --class com.scylladb.migrator.Validator --conf spark.scylla.config=/mnt1/config.yaml /mnt1/scylla-migrator-assembly.jar

-----------------------------------------------
Run the Validator in a Docker-based Spark Setup
-----------------------------------------------

Pass the argument ``--class com.scylladb.migrator.Validator`` to the ``spark-submit`` invocation:

.. code-block:: bash

  docker compose exec spark-master /spark/bin/spark-submit --class com.scylladb.migrator.Validator \
    --master spark://spark-master:7077 \
    --conf spark.driver.host=spark-master \
    --conf spark.scylla.config=/app/config.yaml \
    /jars/scylla-migrator-assembly.jar
