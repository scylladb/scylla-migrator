docker compose exec spark-master \
  /spark/bin/spark-submit \
    --executor-memory 4G \
    --executor-cores 2 \
    --class com.scylladb.migrator.Migrator \
    --master spark://spark-master:7077 \
    --conf spark.driver.host=spark-master \
    --conf spark.scylla.config=/app/config.yaml \
    /app/scylla-migrator-assembly.jar
