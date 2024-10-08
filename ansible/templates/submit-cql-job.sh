#!/bin/bash

set -x

source spark-env

mkdir /tmp/savepoints

time spark-submit --class com.scylladb.migrator.Migrator \
  --master spark://{{ hostvars.spark_master.ansible_default_ipv4.address }}:7077 \
  --conf spark.eventLog.enabled=true \
  --conf spark.scylla.config=config.yaml \
  --conf spark.cassandra.input.consistency.level=LOCAL_QUORUM \
  --conf spark.cassandra.output.consistency.level=LOCAL_QUORUM \
  --executor-memory $EXECUTOR_MEMORY \
  --executor-cores $EXECUTOR_CORES \
  --conf spark.cassandra.connection.localConnectionsPerExecutor=4 \
  /home/ubuntu/scylla-migrator/scylla-migrator-assembly.jar

#sometimes you will need a tuning for driver memory size
#add this config to above to tune it:
#  --driver-memory 4G \

# debug example
#$SPARK_HOME/spark-submit --class com.scylladb.migrator.Migrator \
#  --driver-java-options -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=65005 \
#  --master spark://{{ hostvars.spark_master.ansible_default_ipv4.address }}:7077 \
#  --conf spark.scylla.config=config.yaml \
#  --conf spark.cassandra.input.consistency.level=LOCAL_QUORUM \
#  --conf spark.cassandra.output.consistency.level=LOCAL_QUORUM \
#  --executor-memory $EXECUTOR_MEMORY \
#  --executor-cores $EXECUTOR_CORES \
#  --conf "spark.executor.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=64000 -XX:+HeapDumpOnOutOfMemoryError" \
#  --conf spark.cassandra.connection.localConnectionsPerExecutor=4 \
#  /home/ubuntu/scylla-migrator/scylla-migrator-assembly.jar

#-XX:+HeapDumpOnOutOfMemoryError -XX:+PrintGCDetails