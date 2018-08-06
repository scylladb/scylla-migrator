# Building

Make sure `sbt` is installed on your machine, and run `sbt assembly`.

# Running locally

To run in the local Docker-based setup, first start the environment:
```shell
docker-compose up -d
```

Launch `cqlsh` in Cassandra's container and create a keyspace and a table with some data:
```shell
docker-compose exec cassandra cqlsh
<create stuff>
```

Launch `cqlsh` in Scylla's container and create the destination keyspace and table:
```shell
docker-compose exec scylla cqlsh
<create stuff>
```
The destination table must have the same columns and types as the source table.

Then, launch `spark-submit` in the master's container to run the job:
```shell
docker-compose exec spark-master spark-submit --class com.scylladb.migrator.Migrator \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --conf spark.scylla.source.cluster=cassandra \
  --conf spark.scylla.source.host=cassandra \
  --conf spark.scylla.source.port=9042 \
  --conf spark.scylla.source.keyspace=SOURCE_KEYSPACE \
  --conf spark.scylla.source.table=SOURCE_TABLE \
  --conf spark.scylla.source.splitCount=SOURCE_SPLIT \
  --conf spark.scylla.dest.cluster=scylla \
  --conf spark.scylla.dest.host=scylla \
  --conf spark.scylla.dest.port=9042 \
  --conf spark.scylla.dest.keyspace=DEST_KEYSPACE \
  --conf spark.scylla.dest.table=DEST_TABLE \
  /jars/scylla-migrator-assembly-0.0.1.jar
```

Replace `SOURCE_KEYSPACE`, `SOURCE_TABLE`, `DEST_KEYSPACE`, `DEST_TABLE` and `SOURCE_SPLIT` before running. `SOURCE_SPLIT` will determine how many partitions are used for Spark's execution; ideally this should be the total number of cores in the Spark cluster.

The `spark-master` container mounts the `./target/scala-2.11` dir on `/jars`. To update the jar with new code, just run `sbt assembly` and then run `spark-submit` again.

# Deploying

After running `sbt assembly`, copy the jar from `./target/scala-2.11/scylla-migrator-assembly-0.0.1.jar` to the Spark master server.

There, you can use the same `spark-submit` command, but without the `spark.driver.host` line; that shouldn't be needed. Make sure to adjust the `SOURCE_SPLIT` parameter.
