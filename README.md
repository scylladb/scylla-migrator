# Pre Read 
This is a forked version of the original Migrator jar. We have forked it to make it light weight for our specific use case which was to transfer data from Parquet files in S3 to Scylla in bulk. Additionally, Databricks currently doesn't support instances with Scala version 2.12 so this jar has been downgraded to 2.12 and can be used on Databricks.
# ScyllaDB Migrator

The ScyllaDB Migrator is a Spark application that migrates data to ScyllaDB from CQL-compatible or DynamoDB-compatible databases.

## Documentation

See https://migrator.docs.scylladb.com.

## Building

To test a custom version of the migrator that has not been [released](https://github.com/scylladb/scylla-migrator/releases), you can build it yourself by cloning this Git repository and following the steps below:

1. Make sure the Java 8+ JDK and `sbt` are installed on your machine.
2. Export the `JAVA_HOME` environment variable with the path to the
   JDK installation.
3. Run `build.sh`.
4. This will produce the .jar file to use in the `spark-submit` command at path `migrator/target/scala-2.13/scylla-migrator-assembly.jar`.

## Contributing

Please refer to the file [CONTRIBUTING.md](/CONTRIBUTING.md).
