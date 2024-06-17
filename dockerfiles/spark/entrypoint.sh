#!/bin/bash

export SPARK_NO_DAEMONIZE=true

if [ "$1" == "master" ]
then
  echo "Starting a Spark master node"
  start-master.sh
elif [ "$1" == "worker" ]
then
  echo "Starting a Spark worker node"
  start-worker.sh spark://spark-master:7077
else
  echo "ERROR: Please call the entrypoint with argument 'master' or 'worker'"
  exit 1
fi
