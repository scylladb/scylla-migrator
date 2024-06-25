#!/bin/bash

set -x

source spark-env

mkdir /tmp/savepoints

time spark-submit --class com.scylladb.migrator.Validator \
  --master spark://{{ hostvars.spark_master.ansible_facts.default_ipv4.address }}:7077 \
  --conf spark.eventLog.enabled=true \
  --conf spark.scylla.config=config.yaml \
  --conf spark.cassandra.input.consistency.level=LOCAL_QUORUM \
  --conf spark.cassandra.output.consistency.level=LOCAL_QUORUM \
  --num-executors $SPARK_WORKER_INSTANCES \
  --executor-memory $MEMORY \
  --conf spark.cassandra.connection.localConnectionsPerExecutor=4 \
  migrator/target/scala-2.13/scylla-migrator-assembly.jar