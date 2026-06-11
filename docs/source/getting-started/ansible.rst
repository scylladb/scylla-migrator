===================================
Set Up a Spark Cluster with Ansible
===================================

An `Ansible <https://www.ansible.com/>`_ playbook is provided in the `ansible folder <https://github.com/scylladb/scylla-migrator/tree/master/ansible>`_ of our Git repository. The Ansible playbook installs the prerequisites and Spark on the hosts listed in the inventory that you pass to ``ansible-playbook``. Scylla Migrator will be installed on the Spark master node.

**Target OS**: The Ansible playbook expects the target hosts to use an Ubuntu-compatible Linux distribution. Ubuntu 22.04 LTS and Ubuntu 24.04 LTS are most broadly tested, but other Ubuntu-compatible Linux distributions are likely to work as well.

**Target User**: The Ansible playbook connects to the target hosts via SSH as the user ``ubuntu``, because this is the default user created by most AWS EC2 Ubuntu-based AMIs.

1. Clone the Migrator Git repository:

   .. code-block:: bash

     git clone https://github.com/scylladb/scylla-migrator.git
     cd scylla-migrator/ansible

2. Create an Ansible inventory file with the Spark master and worker instances. The playbook expects ``spark``, ``master``, and ``worker`` groups:

   .. code-block:: ini

     [spark]
     spark_master ansible_host=<master-public-ip> ansible_user=ubuntu
     spark_worker1 ansible_host=<worker-1-public-ip> ansible_user=ubuntu
     spark_worker2 ansible_host=<worker-2-public-ip> ansible_user=ubuntu

     [master]
     spark_master

     [worker]
     spark_worker1
     spark_worker2

3. The ``ansible/templates/spark-env-master`` and ``ansible/templates/spark-env-worker`` templates contain environment variables for Spark driver, executor, and worker resource allocation.
4. Run the playbook, passing the inventory and SSH private key explicitly:

   .. code-block:: bash

     ansible-playbook -i /path/to/inventory.ini --private-key /path/to/private-key -u ubuntu scylla-migrator.yml

5. Start Spark on the Spark master node:

   .. code-block:: bash

     sudo systemctl restart spark-master spark-history-server

6. Start Spark on each Spark worker node:

   .. code-block:: bash

     sudo systemctl restart spark-worker

7. Open Spark web consoles

   - Ensure networking is configured to allow access to the Spark master node via TCP ports 8080, 4040, and 18080.
   - Visit ``http://<spark-master-hostname>:8080`` for the Spark master UI.
   - Visit ``http://<spark-master-hostname>:4040`` for the active Spark application UI after a job starts.
   - Visit ``http://<spark-master-hostname>:18080`` for the Spark history server.

8. `Review and modify config.yaml <./#configure-the-migration>`_ based on whether you're performing a migration to CQL or Alternator.

   - If you're migrating to the ScyllaDB CQL interface (from Apache Cassandra, ScyllaDB, or another CQL source), make a copy of ``../config.yaml.example``, review the comments, and edit as directed.
   - If you're migrating to Alternator (from DynamoDB or another ScyllaDB Alternator source), make a copy of ``files/config.dynamodb.yml``, review the comments, and edit as directed.
   - Copy the final config file to the Spark master under ``/home/ubuntu/scylla-migrator/``. The installed submit scripts expect ``config.yaml`` for CQL migrations and ``config.dynamodb.yml`` for Alternator migrations unless you edit the scripts.

   .. code-block:: bash

     scp -i /path/to/private-key config.yaml ubuntu@<spark-master-hostname>:/home/ubuntu/scylla-migrator/config.yaml
     scp -i /path/to/private-key config.dynamodb.yml ubuntu@<spark-master-hostname>:/home/ubuntu/scylla-migrator/config.dynamodb.yml

9. As part of the Ansible deployment, Spark submit scripts are installed on the master. You may edit and use these scripts.

   - For CQL migration: edit ``/home/ubuntu/scylla-migrator/submit-cql-job.sh`` if your config file is not named ``config.yaml``.
   - For Alternator migration: edit ``/home/ubuntu/scylla-migrator/submit-alternator-job.sh`` if your config file is not named ``config.dynamodb.yml``.

10. Ensure the table has been created in the target environment.
11. Submit the migration by running the appropriate script on the Spark master:

    Use ``nohup`` or a terminal multiplexer such as ``tmux`` when running submit scripts manually, otherwise an SSH disconnection can abort the job.

    .. code-block:: bash

      cd /home/ubuntu/scylla-migrator
      nohup ./submit-cql-job.sh > submit-cql-job.log 2>&1 &

    For Alternator migrations, run ``./submit-alternator-job.sh`` instead. To run validation, use ``./submit-cql-job-validator.sh`` or ``./submit-alternator-validator.sh``.

12. You can monitor progress by observing the Spark web consoles you opened in step 7.

    FYI: When no Spark jobs are actively running, the Spark progress page at port 4040 displays unavailable. It is only useful and renders when a Spark job is in progress.
