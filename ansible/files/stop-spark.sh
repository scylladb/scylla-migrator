#!/bin/bash
set -x

source spark-env

#export SPARK_LOCAL_IP=$SPARK_MASTER_HOST

cd $SPARK_HOME/sbin

./stop-mesos-shuffle-service.sh

./stop-history-server.sh

./stop-master.sh -h $SPARK_MASTER_HOST