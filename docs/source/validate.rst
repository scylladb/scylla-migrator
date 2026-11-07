======================
Validate the Migration
======================

In addition to the monitoring user interface provided by Spark, you can run another program, after running a migration, to check whether the destination table contains exactly the same items as the source table.

Running this program consists of submitting the same Spark job as the migration, but with a different entry point.

For MySQL-to-ScyllaDB validations, run the validator only after writes have stopped on both the
MySQL source table and the ScyllaDB target table. The validator may scan both sides using multiple
statements over time, so it does not provide a single global point-in-time snapshot while live
writes are still happening.

.. _mysql-custom-join-keys:

MySQL custom join keys
----------------------

By default, the MySQL validator checks that ``source.primaryKey`` matches the table's
actual primary key, including column order. To validate using a different join key,
set these properties in the MySQL ``source`` section:

.. code-block:: yaml

  source:
    type: mysql
    # Other MySQL connection settings are unchanged.
    primaryKey: [tenant_id, external_id]
    skipSourcePrimaryKeyValidation: true
    verifySourceJoinKeyUniqueness: true

The configured columns must exist in the source. After applying ``renames``, they
must match the ScyllaDB target primary key. The override bypasses only the check
against MySQL's declared primary key; it does not bypass the target check.

All three custom-key options default to ``false``. When
``skipSourcePrimaryKeyValidation`` is enabled, choose one of these safeguards:

* Set ``verifySourceJoinKeyUniqueness: true`` to scan the selected source rows and
  reject duplicate key combinations or null key components before target validation.
  This adds a full scan and shuffle. Source rows are persisted in Spark memory and
  disk for reuse during validation, then released on success or failure. An existing
  caller-owned persistence setting is preserved.
* Set ``acknowledgeUnverifiedJoinKey: true`` to explicitly accept proceeding without
  that scan. Do this only after independently confirming that the configured key is
  unique and non-null. Conflicting duplicate source rows can produce field mismatches
  against one target row. With ``copyMissingRows``, duplicates can be written under the
  same target key, where the last write wins. This acknowledgment does not establish
  uniqueness.

Validation refuses to use a custom join key if neither safeguard is selected. If
both are enabled, verification takes precedence. The two safeguard options have no
effect when ``skipSourcePrimaryKeyValidation`` is ``false``. These options affect
validation only; they do not change how migration reads MySQL data. A source
``where`` filter limits verification to the selected rows. Stop writes on both sides
as described above, including throughout the uniqueness check.

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
