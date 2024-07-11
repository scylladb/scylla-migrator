================================
Manual Set Up of a Spark Cluster
================================

This page describes how to set up a Spark cluster on your infrastructure and to use it to perform a migration.

1. Follow the `official documentation <https://spark.apache.org/docs/latest/spark-standalone.html>`_ to install Spark on each node of your cluster, and start the Spark master and the Spark workers.

2. In the Spark master node, download the latest release of the Migrator.

   .. code-block:: bash

     wget https://github.com/scylladb/scylla-migrator/releases/latest/download/scylla-migrator-assembly.jar

   Alternatively, `download a specific release of scylla-migrator-assembly.jar <https://github.com/scylladb/scylla-migrator/releases>`_.

3. In the Spark master node, copy the file ``config.yaml.example`` from our Git repository.

   .. code-block:: bash

     wget https://github.com/scylladb/scylla-migrator/raw/master/config.yaml.example \
       --output-document=config.yaml

4. `Configure the migration <../#configure-the-migration>`_ according to your needs.

5. Finally, run the migration as follows from the Spark master node.

   .. code-block:: bash

     spark-submit --class com.scylladb.migrator.Migrator \
       --master spark://<spark-master-hostname>:7077 \
       --conf spark.scylla.config=<path to config.yaml> \
       <path to scylla-migrator-assembly.jar>

   See also our `general recommendations to tune the Spark job <../#run-the-migration>`_.

6. You can monitor progress from the `Spark web UI <https://spark.apache.org/docs/latest/spark-standalone.html#monitoring-and-logging>`_.
