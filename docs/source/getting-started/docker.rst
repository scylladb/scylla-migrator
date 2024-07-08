==================================
Set Up a Spark Cluster with Docker
==================================

This page describes how to set up a Spark cluster locally on your machine by using Docker containers. This approach is useful if you do not need a high-level of performance, and want to quickly try out the Migrator without having to set up a real cluster of nodes. It requires Docker and Git.

1. Clone the Migrator repository.

   .. code-block:: bash

     git clone https://github.com/scylladb/scylla-migrator.git
     cd scylla-migrator

2. Download the latest release of the ``scylla-migrator-assembly.jar`` and put it in the directory ``migrator/target/scala-2.13/``.

   .. code-block:: bash

     mkdir -p migrator/target/scala-2.13
     wget https://github.com/scylladb/scylla-migrator/releases/latest/download/scylla-migrator-assembly.jar \
       --directory-prefix=migrator/target/scala-2.13

   Alternatively, `download a specific release of scylla-migrator-assembly.jar <https://github.com/scylladb/scylla-migrator/releases>`_.

3. Start the Spark cluster.

   .. code-block:: bash

     docker compose up -d

4. Open the Spark web UI.

   http://localhost:8080

   Tip: add the following aliases to your ``/etc/hosts`` to make links work in the Spark UI

   .. code-block:: text

     127.0.0.1   spark-master
     127.0.0.1   spark-worker

5. Rename the file ``config.yaml.example`` to ``config.yaml``, and `configure </getting-started/#configure-the-migration>`_ it according to your needs.

6. Finally, run the migration.

   .. code-block:: bash

     docker compose exec spark-master /spark/bin/spark-submit --class com.scylladb.migrator.Migrator \
       --master spark://spark-master:7077 \
       --conf spark.driver.host=spark-master \
       --conf spark.scylla.config=/app/config.yaml \
       /jars/scylla-migrator-assembly.jar

   The ``spark-master`` container mounts the ``./migrator/target/scala-2.13`` dir on ``/jars`` and the repository root on ``/app``.

7. You can monitor progress by observing the Spark web console you opened in step 4. Additionally, after the job has started, you can track progress via ``http://localhost:4040``.

    FYI: When no Spark jobs are actively running, the Spark progress page at port 4040 displays unavailable. It is only useful and renders when a Spark job is in progress.
