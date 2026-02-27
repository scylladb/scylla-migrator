package com.scylladb.migrator

import scala.sys.process.{ Process, ProcessBuilder }

object SparkUtils {

  @volatile private var sharedSession: SharedSparkSession = null

  private class SharedSparkSession {
    private val process: java.lang.Process = {
      val pb = new java.lang.ProcessBuilder(
        "docker",
        "compose",
        "-f",
        "../docker-compose-tests.yml",
        "exec",
        "-T",
        "spark-master",
        "/spark/bin/spark-submit",
        "--class",
        "com.scylladb.migrator.InteractiveMigrator",
        "--master",
        "spark://spark-master:7077",
        "--conf",
        "spark.driver.host=spark-master",
        "--executor-cores",
        "2",
        "--executor-memory",
        "4G",
        "/jars/scylla-migrator-assembly.jar"
      )
      pb.redirectError(java.lang.ProcessBuilder.Redirect.INHERIT)
      pb.start()
    }

    private val writer =
      new java.io.PrintWriter(process.getOutputStream, true)
    private val reader =
      new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream))

    // Wait for READY signal from InteractiveMigrator
    locally {
      val readyLine = reader.readLine()
      assert(readyLine == "READY", s"Expected READY from InteractiveMigrator but got: $readyLine")
    }

    def runMigration(configFile: String): Unit = synchronized {
      writer.println(s"/app/configurations/$configFile")
      writer.flush()
      val result = reader.readLine()
      if (result == null)
        throw new AssertionError("InteractiveMigrator process terminated unexpectedly")
      assert(result == "OK", s"Migration failed: $result")
    }

    def runValidation(configFile: String): Int = synchronized {
      writer.println(s"VALIDATE:/app/configurations/$configFile")
      writer.flush()
      val result = reader.readLine()
      if (result == null)
        throw new AssertionError("InteractiveMigrator process terminated unexpectedly")
      if (result.startsWith("OK:")) result.substring(3).toInt
      else throw new AssertionError(s"Validation failed: $result")
    }

    def close(): Unit =
      try {
        writer.println("EXIT")
        writer.flush()
        process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)
      } catch { case _: Exception => () }
      finally process.destroyForcibly()
  }

  private def getOrCreateSession(): SharedSparkSession = {
    if (sharedSession == null) {
      SparkUtils.synchronized {
        if (sharedSession == null) {
          sharedSession = new SharedSparkSession()
          Runtime.getRuntime.addShutdownHook(new Thread(() => stopSharedSession()))
        }
      }
    }
    sharedSession
  }

  /** Run a migration by submitting it to the shared InteractiveMigrator session and waiting for its
    * successful completion.
    *
    * @param migratorConfigFile
    *   Configuration file to use. Write your configuration files in the directory
    *   `src/test/configurations`, which is automatically mounted to the Spark cluster by Docker
    *   Compose.
    */
  def successfullyPerformMigration(migratorConfigFile: String): Unit =
    getOrCreateSession().runMigration(migratorConfigFile)

  /** Run validation via the shared InteractiveMigrator session.
    *
    * @return
    *   0 if validation passed (no differences found), 1 if validation failures were detected
    */
  def performValidation(migratorConfigFile: String): Int =
    getOrCreateSession().runValidation(migratorConfigFile)

  /** Shut down the shared InteractiveMigrator session. Called automatically via shutdown hook. */
  def stopSharedSession(): Unit = SparkUtils.synchronized {
    if (sharedSession != null) {
      sharedSession.close()
      sharedSession = null
    }
  }

  /** Submit a Spark job as a separate process. Use this for streaming jobs that need to run
    * asynchronously (e.g., DynamoDB Streams replication).
    */
  def submitSparkJob(migratorConfigFile: String, entryPoint: String): Process =
    submitSparkJobProcess(migratorConfigFile, entryPoint).run()

  /** @param migratorConfigFile
    *   Configuration file to use
    * @param entryPoint
    *   Java entry point of the job
    * @return
    *   The running process
    */
  def submitSparkJobProcess(migratorConfigFile: String, entryPoint: String): ProcessBuilder =
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
        "--executor-cores",
        "2",
        "--executor-memory",
        "4G",
        "/jars/scylla-migrator-assembly.jar"
      )
    )

}
