#!/bin/bash

set -x

source spark-env

mkdir /tmp/savepoints

time spark-submit --class com.scylladb.migrator.Validator \
  --master spark://{{ hostvars.spark_master.ansible_default_ipv4.address }}:7077 \
  --conf spark.eventLog.enabled=true \
  --conf spark.scylla.config=config.yaml \
  --conf spark.cassandra.input.consistency.level=LOCAL_QUORUM \
  --conf spark.cassandra.output.consistency.level=LOCAL_QUORUM \
  --executor-memory $EXECUTOR_MEMORY \
  --executor-cores $EXECUTOR_CORES \
  --conf spark.cassandra.connection.localConnectionsPerExecutor=4 \
  /home/ubuntu/scylla-migrator/scylla-migrator-assembly.jar