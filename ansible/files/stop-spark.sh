#!/bin/bash
set -x

. spark-env

export SPARK_LOCAL_IP=$SPARK_MASTER_HOST

cd s$SPARK_HOME/sbin

./stop-shuffle-service.sh
./stop-slave.sh spark://$SPARK_MASTER_HOST:7077 $SLAVESIZE

./stop-history-server.sh

./stop-master.sh -h $SPARK_MASTER_HOST