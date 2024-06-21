# Ansible deployment

An ansible playbook is provided in ansible folder.  The ansible playbook will install the pre-requisites, spark, on the master and workers added to the `ansible/inventory/hosts` file.  Scylla-migrator will be installed on the spark master node.
1. Update `ansible/inventory/hosts` file with master and worker instances
2. Update `ansible/ansible.cfg` with location of private key if necessary
3. The `ansible/template/spark-env-master-sample` and `ansible/template/spark-env-worker-sample` contain environment variables determining number of workers, CPUs per worker, and memory allocations - as well as considerations for setting them.
4. run `ansible-playbook scylla-migrator.yml`
5. On the spark master node:
  cd scylla-migrator
  `./start-spark.sh`
6. On the spark worker nodes:
  `./start-slave.sh`
7. Open spark web console
  - Ensure networking is configured to allow you access spark master node via 8080 and 4040
  - visit http://<spark-master-hostname>:8080
8. Review and modify `config.yaml` based whether you're performing a migration to CQL or Alternator
  - If you're migrating to Scylla CQL interface (from Cassandra, Scylla, or other CQL source), make a copy review the comments in `config.yaml.example`, and edit as directed.
  - If you're migrating to Alternator (from DynamoDB or other Scylla Alternator), make a copy, review the comments in `config.dynamodb.yml`, and edit as directed.
9. As part of ansible deployment, sample submit jobs were created.  You may edit and use the submit jobs.
  - For CQL migration: Edit `scylla-migrator/submit-cql-job.sh`, change line `--conf spark.scylla.config=config.yaml \` to point to the whatever you named the config.yaml in previous step.
  - For Alternator migration: Edit `scylla-migrator/submit-alternator-job.sh`, change line `--conf spark.scylla.config=/home/ubuntu/scylla-migrator/config.dynamodb.yml \` to reference the config.yaml file you created and modified in previous step.
10. Ensure the table has been created in the target environment.
11. Submit the migration by submitting the appropriate job
  - CQL migration: `./submit-cql-job.sh`
  - Alternator migration: `./submit-alternator-job.sh`
12. You can monitor progress by observing the spark web console you opened in step 7.  Additionally, after the job has started, you can track progress via http://<spark-master-hostname>:4040.  
  FYI: When no spark jobs are actively running, the spark progress page at port 4040 displays unavailable.  It is only useful and renders when a spark job is in progress.


# Building

1. Make sure the Java 8+ JDK and `sbt` are installed on your machine.
2. Export the `JAVA_HOME` environment variable with the path to the
JDK installation.
3. Run `build.sh`.

# Configuring the Migrator

Create a `config.yaml` for your migration using the template `config.yaml.example` in the repository root. Read the comments throughout carefully.

# Running on a live Spark cluster

The Scylla Migrator is built against Spark 3.5.1, so you'll need to run that version on your cluster.

If you didn't build Scylla Migrator on the master node:
After running `build.sh`, copy the jar from `./migrator/target/scala-2.13/scylla-migrator-assembly-0.0.1.jar` and the `config.yaml` you've created to the Spark master server.

Start the spark master and slaves.
`cd scylla-migrator`
`./start-spark.sh`

On worker instances:
`./start-slave.sh`

Configure and confirm networking between:
- source and spark servers
- target and spark servers

Create schema in target server.

Then, run this command on the Spark master server:
```shell
spark-submit --class com.scylladb.migrator.Migrator \
  --master spark://<spark-master-hostname>:7077 \
  --conf spark.scylla.config=<path to config.yaml> \
  <path to scylla-migrator-assembly-0.0.1.jar>
```

If you pass on the truststore file or ssl related files use `--files` option:
```shell
spark-submit --class com.scylladb.migrator.Migrator \
  --master spark://<spark-master-hostname>:7077 \
  --conf spark.scylla.config=<path to config.yaml> \
  --files truststorefilename \
  <path to scylla-migrator-assembly-0.0.1.jar>
```

# Running the validator

This project also includes an entrypoint for comparing the source
table and the target table. You can launch it as so (after performing
the previous steps):

```shell
spark-submit --class com.scylladb.migrator.Validator \
  --master spark://<spark-master-hostname>:7077 \
  --conf spark.scylla.config=<path to config.yaml> \
  <path to scylla-migrator-assembly-0.0.1.jar>
```

# Running locally

To run in the local Docker-based setup:

1. First start the environment:
```shell
docker compose up -d
```

2. Launch `cqlsh` in Cassandra's container and create a keyspace and a table with some data:
```shell
docker compose exec cassandra cqlsh
<create stuff>
```

3. Launch `cqlsh` in Scylla's container and create the destination keyspace and table with the same schema as the source table:
```shell
docker compose exec scylla cqlsh
<create stuff>
```

4. Edit the `config.yaml` file; note the comments throughout.

5. Run `build.sh`.

6. Then, launch `spark-submit` in the master's container to run the job:
```shell
docker compose exec spark-master /spark/bin/spark-submit --class com.scylladb.migrator.Migrator \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --conf spark.scylla.config=/app/config.yaml \
  /jars/scylla-migrator-assembly-0.0.1.jar
```

The `spark-master` container mounts the `./migrator/target/scala-2.13` dir on `/jars` and the repository root on `/app`. To update the jar with new code, just run `build.sh` and then run `spark-submit` again.
