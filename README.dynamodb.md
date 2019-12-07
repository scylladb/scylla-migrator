# Building

Make sure `sbt` is installed on your machine, and run `build.sh`.

# Configuring the Migrator

Create a `config.dynamodb.yaml` for your migration using the template `config.dynamodb.yaml` in the repository root. Read the comments throughout carefully.
Proper setting of this file will have serious impact on your performance and resources that are needed from spark workers!

# Running on a live Spark cluster

The Scylla Migrator is built against Spark 2.3.1, so you'll need to run that version on your cluster.

After running `build.sh`, copy the jar from `./target/scala-2.11/scylla-migrator-assembly-0.0.1.jar` and the `config.dynamodb.yaml` you've created to the Spark master server.

Then, run this command on the Spark master server:
```shell
spark-submit --class com.scylladb.migrator.DynamoDBMigrator \
  --master spark://<spark-master-hostname>:7077 \
  --conf spark.scylla.config=<path to config.dynamodb.yaml>
  <path to scylla-migrator-assembly-0.0.1.jar>
```

It is suggested to increase the memory above defaults and depending on your config.
E.g. you might add to above ```--conf "spark.executor.memory=100G" \```. 
Alternative is to modify your defaults in your spark config and add enough resources.

# Running locally

To run in the local Docker-based setup:

1. First start the environment:
```shell
docker-compose up -d
```

2. Launch `cqlsh` in Cassandra's container and create a keyspace and a table with some data:
```shell
docker-compose exec cassandra cqlsh
<create stuff>
```

3. Launch `cqlsh` in Scylla's container and create the destination keyspace and table with the same schema as the source table:
```shell
docker-compose exec scylla cqlsh
<create stuff>
```

4. Edit the `config.yaml` file; note the comments throughout.

5. Run `build.sh`.

6. Then, launch `spark-submit` in the master's container to run the job:
```shell
docker-compose exec spark-master spark-submit --class com.scylladb.migrator.DynamoDBMigrator \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --conf spark.scylla.config=/app/config.dynamodb.yaml \
  /jars/scylla-migrator-assembly-0.0.1.jar
```

The `spark-master` container mounts the `./target/scala-2.11` dir on `/jars` and the repository root on `/app`. To update the jar with new code, just run `build.sh` and then run `spark-submit` again.
