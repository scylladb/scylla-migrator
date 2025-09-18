# ScyllaDB Migrator

The ScyllaDB Migrator is a Spark application that migrates data to ScyllaDB from CQL-compatible or DynamoDB-compatible databases.

## Documentation

See https://migrator.docs.scylladb.com.

## Building

To test a custom version of the migrator that has not been [released](https://github.com/scylladb/scylla-migrator/releases), you can build it yourself by cloning this Git repository and following the steps below:

Build locally:
1. Make sure the Java 8+ JDK and `sbt` are installed on your machine.
2. Export the `JAVA_HOME` environment variable with the path to the JDK installation.
3. Run `build.sh`

Build Locally in Docker:
1. Run `docker-build-jar.sh` to build locally using docker or
   
Both options will produce the .jar file to use in `spark-submit` command at path `migrator/target/scala-2.13/scylla-migrator-assembly.jar`.

## Contributing

Please refer to the file [CONTRIBUTING.md](/CONTRIBUTING.md).
