#!/bin/bash
set -x

source spark-env

#export SPARK_LOCAL_IP=$SPARK_MASTER_HOST

cd $SPARK_HOME/sbin

if [[ -x ./stop-mesos-shuffle-service.sh ]]; then
  ./stop-mesos-shuffle-service.sh
else
  echo "Mesos shuffle service is not available in this Spark distribution; skipping."
fi

./stop-history-server.sh

./stop-master.sh -h $SPARK_MASTER_HOST