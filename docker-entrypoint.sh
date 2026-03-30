#!/bin/bash
exec /opt/spark/bin/spark-submit \
  --class "${SPARK_CLASS:-com.scylladb.migrator.Migrator}" \
  --master "${SPARK_MASTER:-local[*]}" \
  "$@" \
  /jars/scylla-migrator-assembly.jar
