package com.scylladb.migrator

import scala.sys.process.Process

object SparkUtils {

  /**
   * Run a migration by submitting a Spark job to the Spark cluster
   * and waiting for its successful completion.
   *
   * @param configFile Configuration file to use. Write your
   *                   configuration files in the directory
   *                   `src/test/configurations`, which is
   *                   automatically mounted to the Spark
   *                   cluster by Docker Compose.
   */
  def submitMigrationJob(configFile: String): Unit = {
    val process =
      Process(
        Seq(
          "docker",
          "compose",
          "-f", "docker-compose-tests.yml",
          "exec",
          "spark-master",
          "/spark/bin/spark-submit",
          "--class", "com.scylladb.migrator.Migrator",
          "--master", "spark://spark-master:7077",
          "--conf", "spark.driver.host=spark-master",
          "--conf", s"spark.scylla.config=/app/configurations/${configFile}",
          // Uncomment one of the following lines to plug a remote debugger on the Spark master or worker.
          // "--conf", "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005",
          // "--conf", "spark.executor.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006",
          "/jars/scylla-migrator-assembly-0.0.1.jar"
        )
      )
    process
      .run()
      .exitValue()
      .ensuring(statusCode => statusCode == 0, "Spark job failed")
    ()
  }

}
