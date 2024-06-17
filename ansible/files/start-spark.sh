#!/bin/bash
set -x

source spark-env

#export SPARK_LOCAL_IP=$SPARK_MASTER_HOST

cd $SPARK_HOME/sbin

./start-master.sh

/bin/mkdir /tmp/spark-events

./start-history-server.sh

./start-slave.sh spark://$SPARK_MASTER_HOST:7077 $SLAVESIZE

./start-shuffle-service.sh