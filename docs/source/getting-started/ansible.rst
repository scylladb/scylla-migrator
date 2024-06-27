===================================
Set Up a Spark Cluster with Ansible
===================================

An `Ansible <https://www.ansible.com/>`_ playbook is provided in the `ansible <https://github.com/scylladb/scylla-migrator/tree/master/ansible>`_ folder of our Git repository.  The Ansible playbook will install the pre-requisites, Spark, on the master and workers added to the ``ansible/inventory/hosts`` file.  Scylla-migrator will be installed on the spark master node.

1. Update ``ansible/inventory/hosts`` file with master and worker instances
2. Update ``ansible/ansible.cfg`` with location of private key if necessary
3. The ``ansible/template/spark-env-master-sample`` and ``ansible/template/spark-env-worker-sample`` contain environment variables determining number of workers, CPUs per worker, and memory allocations - as well as considerations for setting them.
4. run ``ansible-playbook scylla-migrator.yml``
5. On the Spark master node: ::

     cd scylla-migrator
     ./start-spark.sh

6. On the Spark worker nodes: ::

     ./start-slave.sh

7. Open Spark web console

   - Ensure networking is configured to allow you access spark master node via TCP ports 8080 and 4040
   - visit ``http://<spark-master-hostname>:8080``

8. Review and modify ``config.yaml`` based whether you're performing a migration to CQL or Alternator

   - If you're migrating to Scylla CQL interface (from Cassandra, Scylla, or other CQL source), make a copy review the comments in ``config.yaml.example``, and edit as directed.
   - If you're migrating to Alternator (from DynamoDB or other Scylla Alternator), make a copy, review the comments in ``config.dynamodb.yml``, and edit as directed.

9. As part of ansible deployment, sample submit jobs were created.  You may edit and use the submit jobs.

   - For CQL migration: edit ``scylla-migrator/submit-cql-job.sh``, change line ``--conf spark.scylla.config=config.yaml \`` to point to the whatever you named the ``config.yaml`` in previous step.
   - For Alternator migration: edit ``scylla-migrator/submit-alternator-job.sh``, change line ``--conf spark.scylla.config=/home/ubuntu/scylla-migrator/config.dynamodb.yml \`` to reference the ``config.yaml`` file you created and modified in previous step.

10. Ensure the table has been created in the target environment.
11. Submit the migration by submitting the appropriate job

    - CQL migration: ``./submit-cql-job.sh``
    - Alternator migration: ``./submit-alternator-job.sh``

12. You can monitor progress by observing the Spark web console you opened in step 7. Additionally, after the job has started, you can track progress via ``http://<spark-master-hostname>:4040``.

    FYI: When no Spark jobs are actively running, the Spark progress page at port 4040 displays unavailable. It is only useful and renders when a Spark job is in progress.
