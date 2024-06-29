===================================
Set Up a Spark Cluster with AWS EMR
===================================

This page describes how to use the Migrator in `Amazon EMR <https://aws.amazon.com/emr/>`_. This approach is useful if you already have an AWS account, or if you do not want to manage your infrastructure manually.

1. Download the ``config.yaml.example`` from our Git repository.

   .. code-block:: bash

     wget https://github.com/scylladb/scylla-migrator/raw/master/config.yaml.example \
       --output-document=config.yaml


2. `Configure the migration </getting-started/#configure-the-migration>`_ according to your needs.

3. Download the latest release of the Migrator.

   .. code-block:: bash

     wget https://github.com/scylladb/scylla-migrator/releases/latest/download/scylla-migrator-assembly.jar

4. Upload them to an S3 bucket.

   .. code-block:: bash

     aws s3 cp config.yaml s3://<your-bucket>/scylla-migrator/config.yaml
     aws s3 cp scylla-migrator-assembly.jar s3://<your-bucket>/scylla-migrator/scylla-migrator-assembly.jar

   Replace ``<your-bucket>`` with an S3 bucket name that you manage.

   Each time you change the migration configuration, re-upload it to the bucket.

4. Create a script named ``copy-files.sh``, to load the files ``config.yaml`` and ``scylla-migrator-assembly.jar`` from your S3 bucket.

   .. code-block:: bash

     #!/bin/bash
     aws s3 cp s3://<your-bucket>/scylla-migrator/config.yaml /mnt1/config.yaml
     aws s3 cp s3://<your-bucket>/scylla-migrator/scylla-migrator-assembly.jar /mnt1/scylla-migrator-assembly.jar

5. Upload the script to your S3 bucket as well.

   .. code-block:: bash

     aws s3 cp copy-files.sh s3://<your-bucket>/scylla-migrator/copy-files.sh

6. Log in to the `AWS EMR console <https://console.aws.amazon.com/emr>`_.

7. Choose “Create cluster” to create a new cluster based on EC2.

8. Configure the cluster as follows:

   - Choose the EMR release ``emr-7.1.0``, or any EMR release that is compatible with the Spark version used by the Migrator.
   - Make sure to include Spark in the application bundle.
   - Choose all-purpose EC2 instance types (e.g., i4i).
   - Make sure to include at least one task node.
   - Add a Step to run the Migrator:

     - Type: Custom JAR
     - JAR location: ``command-runner.jar``
     - Arguments:

       .. code-block:: text

         spark-submit --deploy-mode cluster --class com.scylladb.migrator.Migrator --conf spark.scylla.config=/mnt1/config.yaml /mnt1/scylla-migrator-assembly.jar

   - Add a Bootstrap action to download the Migrator and the migration configuration:

     - Script location: ``s3://<your-bucket>/scylla-migrator/copy-files.sh``

9. Finalize your cluster configuration according to your needs and finally choose “Create cluster”.

10. The migration will start automatically after the cluster is fully up.
