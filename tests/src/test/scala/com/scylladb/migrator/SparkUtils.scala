package com.scylladb.migrator

import scala.sys.process.Process

object SparkUtils {

  /**
    * Run a migration by submitting a Spark job to the Spark cluster
    * and waiting for its successful completion.
    *
    * @param migratorConfigFile Configuration file to use. Write your
    *                           configuration files in the directory
    *                           `src/test/configurations`, which is
    *                           automatically mounted to the Spark
    *                           cluster by Docker Compose.
    */
  def successfullyPerformMigration(migratorConfigFile: String): Unit = {
    submitSparkJob(migratorConfigFile, "com.scylladb.migrator.Migrator")
      .exitValue()
      .ensuring(statusCode => statusCode == 0, "Spark job failed")
    ()
  }

  /**
    * @param migratorConfigFile Configuration file to use
    * @param entryPoint         Java entry point of the job
    * @return The running process
    */
  def submitSparkJob(migratorConfigFile: String, entryPoint: String): Process =
    Process(
      Seq(
        "docker",
        "compose",
        "-f",
        "../docker-compose-tests.yml",
        "exec",
        "spark-master",
        "/spark/bin/spark-submit",
        "--class",
        entryPoint,
        "--master",
        "spark://spark-master:7077",
        "--conf",
        "spark.driver.host=spark-master",
        "--conf",
        s"spark.scylla.config=/app/configurations/${migratorConfigFile}",
        "--executor-cores", "2",
        "--executor-memory", "4G",
        // Uncomment one of the following lines to plug a remote debugger on the Spark master or worker.
        // "--conf", "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005",
        // "--conf", "spark.executor.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5006",
        "/jars/scylla-migrator-assembly.jar"
      )
    ).run()

}
