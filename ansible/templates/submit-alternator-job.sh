#!/bin/bash

set -x

source spark-env

mkdir /tmp/savepoints

# 

time spark-submit --class com.scylladb.migrator.Migrator \
--master spark://{{ hostvars.spark_master.ansible_facts.default_ipv4.address }}:7077 \
--conf spark.eventLog.enabled=true \
--conf spark.scylla.config=/home/ubuntu/scylla-migrator/config.dynamodb.yml \
--conf spark.executor.memory=$MEMORY \
--conf spark.driver.memory=64G \
/home/ubuntu/scylla-migrator/migrator/target/scala-2.13/scylla-migrator-assembly-0.0.1.jar