#!/bin/bash

set -x

source spark-env

mkdir /tmp/savepoints

time spark-submit --class com.scylladb.migrator.Validator \
--master spark://{{ hostvars.spark_master.ansible_default_ipv4.address }}:7077 \
--conf spark.eventLog.enabled=true \
--conf spark.scylla.config=/home/ubuntu/scylla-migrator/config.dynamodb.yml \
--executor-memory $EXECUTOR_MEMORY \
--executor-cores $EXECUTOR_CORES \
--driver-memory 4G \
/home/ubuntu/scylla-migrator/scylla-migrator-assembly.jar
