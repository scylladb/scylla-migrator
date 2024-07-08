# Running on a live Spark cluster

The Scylla Migrator is built against Spark 3.5.1, so you'll need to run that version on your cluster.

Download the latest [release](https://github.com/scylladb/scylla-migrator/releases) of the migrator:

~~~ sh
wget https://github.com/scylladb/scylla-migrator/releases/latest/download/scylla-migrator-assembly.jar
~~~

Alternatively, you can [build](#building) a custom version of the migrator.

Copy the jar `scylla-migrator-assembly.jar` and the `config.yaml` you've created to the Spark master server.

Start the spark master and slaves.
`cd scylla-migrator`
`./start-spark.sh`

On worker instances:
`./start-slave.sh`

Configure and confirm networking between:
- source and spark servers
- target and spark servers

Create schema in target server.

Then, run this command on the Spark master server:
```shell
spark-submit --class com.scylladb.migrator.Migrator \
  --master spark://<spark-master-hostname>:7077 \
  --conf spark.scylla.config=<path to config.yaml> \
  <path to scylla-migrator-assembly.jar>
```

If you pass on the truststore file or ssl related files use `--files` option:
```shell
spark-submit --class com.scylladb.migrator.Migrator \
  --master spark://<spark-master-hostname>:7077 \
  --conf spark.scylla.config=<path to config.yaml> \
  --files truststorefilename \
  <path to scylla-migrator-assembly.jar>
```

# Documentation

See https://migrator.docs.scylladb.com.

# Building

To test a custom version of the migrator that has not been [released](https://github.com/scylladb/scylla-migrator/releases), you can build it yourself by cloning this Git repository and following the steps below:

1. Make sure the Java 8+ JDK and `sbt` are installed on your machine.
2. Export the `JAVA_HOME` environment variable with the path to the
   JDK installation.
3. Run `build.sh`.
4. This will produce the .jar file to use in the `spark-submit` command at path `migrator/target/scala-2.13/scylla-migrator-assembly.jar`.
