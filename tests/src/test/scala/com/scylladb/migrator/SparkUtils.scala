package com.scylladb.migrator

import com.scylladb.migrator.config.{
  DynamoDBEndpoint,
  MigratorConfig,
  SourceSettings,
  TargetSettings
}
import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NonFatal

object SparkUtils {

  /** Handle to a migration running in a background thread. */
  class BackgroundMigration(thread: Thread, exitCode: AtomicInteger) {

    /** Block until the migration finishes and return its exit code. For streaming migrations, this
      * will block indefinitely unless `stop()` is called first.
      */
    def exitValue(): Int = {
      thread.join()
      exitCode.get()
    }

    /** Stop the background migration by cancelling active Spark streaming queries. */
    def stop(): Unit = {
      spark.streams.active.foreach(_.stop())
      thread.join(30000)
      if (thread.isAlive) {
        thread.interrupt()
        thread.join(5000)
      }
    }
  }

  /** Shared SparkSession in local mode, created once and reused across all tests. */
  private lazy val spark: SparkSession = {
    Configurator.setRootLevel(Level.WARN)
    Configurator.setLevel("com.scylladb.migrator", Level.INFO)
    Configurator.setLevel("org.apache.spark.scheduler.TaskSetManager", Level.WARN)
    Configurator.setLevel("com.datastax.spark.connector.cql.CassandraConnector", Level.WARN)

    SparkSession
      .builder()
      .master("local[*]")
      .appName("integration-tests")
      .config("spark.driver.host", "localhost")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .getOrCreate()
  }

  /** Run a migration by loading the config, remapping for local execution, and running in-process.
    *
    * @param migratorConfigFile
    *   Configuration file to use. Write your configuration files in the directory
    *   `src/test/configurations`.
    */
  def successfullyPerformMigration(migratorConfigFile: String): Unit = {
    val config = loadAndRemapConfig(migratorConfigFile)
    Migrator.migrate(config)(spark)
  }

  /** Run validation and return the exit code.
    *
    * @return
    *   0 if validation passed (no differences found), 1 if validation failures were detected
    */
  def performValidation(migratorConfigFile: String): Int = {
    val config = loadAndRemapConfig(migratorConfigFile)
    val failures: List[RowComparisonFailure] = Validator.runValidation(config)(spark)
    if (failures.isEmpty) 0 else 1
  }

  /** Start a migration in a background thread and return a handle to stop it.
    *
    * This is useful for streaming migrations that run indefinitely until explicitly stopped.
    *
    * @param logCallback
    *   Optional callback invoked with log messages from the migration. Note: in-process execution
    *   shares the JVM log output; this callback receives migration log events via a custom
    *   appender.
    */
  def submitMigrationInBackground(
    migratorConfigFile: String,
    logCallback: String => Unit = _ => ()
  ): BackgroundMigration = {
    val config = loadAndRemapConfig(migratorConfigFile)
    val exitCode = new AtomicInteger(0)
    val thread = new Thread(() =>
      try
        Migrator.migrate(config)(spark)
      catch {
        case _: InterruptedException => exitCode.set(143)
        case NonFatal(e) =>
          exitCode.set(1)
          e.printStackTrace()
      }
    )
    thread.setDaemon(true)
    thread.setName(s"migration-$migratorConfigFile")
    thread.start()
    new BackgroundMigration(thread, exitCode)
  }

  /** Resolve the full path to a config file in the test configurations directory. */
  private def resolveConfigPath(configFile: String): String =
    Paths.get("src", "test", "configurations", configFile).toAbsolutePath.toString

  /** Load a config file and remap Docker hostnames/ports/paths for localhost execution. */
  private def loadAndRemapConfig(configFile: String): MigratorConfig = {
    val config = MigratorConfig.loadFrom(resolveConfigPath(configFile))
    remapForLocalExecution(config)
  }

  // ---------------------------------------------------------------------------
  // Docker → localhost remapping
  // ---------------------------------------------------------------------------

  /** Map from Docker Compose service hostnames to the host port exposed for CQL (9042 inside). */
  private val cqlHostPortMap: Map[String, Int] = Map(
    "cassandra"     -> 9043,
    "cassandra2"    -> 9046,
    "cassandra3"    -> 9045,
    "cassandra5"    -> 9047,
    "scylla"        -> 9042,
    "scylla-source" -> 9044
  )

  /** Map from Docker Compose DynamoDB-protocol endpoint to (host, port) on localhost. */
  private val dynamoEndpointMap: Map[(String, Int), (String, Int)] = Map(
    ("http://dynamodb", 8000) -> ("http://localhost", 8001),
    ("http://scylla", 8000)   -> ("http://localhost", 8000),
    ("http://s3", 4566)       -> ("http://localhost", 4566)
  )

  private def remapForLocalExecution(config: MigratorConfig): MigratorConfig = {
    val newSource = config.source match {
      case c: SourceSettings.Cassandra if c.cloud.isEmpty =>
        c.copy(host = "localhost", port = cqlHostPortMap.getOrElse(c.host, c.port))
      case c: SourceSettings.Cassandra => c
      case d: SourceSettings.DynamoDB =>
        d.copy(endpoint = d.endpoint.map(remapEndpoint))
      case a: SourceSettings.Alternator =>
        a.copy(alternatorEndpoint = remapEndpoint(a.alternatorEndpoint))
      case p: SourceSettings.Parquet =>
        p.copy(
          path     = remapContainerPath(p.path),
          endpoint = p.endpoint.map(remapEndpoint)
        )
      case s: SourceSettings.DynamoDBS3Export =>
        s.copy(endpoint = s.endpoint.map(remapEndpoint))
    }
    val newTarget = config.target match {
      case s: TargetSettings.Scylla if s.cloud.isEmpty =>
        s.copy(host = "localhost", port = cqlHostPortMap.getOrElse(s.host, s.port))
      case s: TargetSettings.Scylla => s
      case d: TargetSettings.DynamoDB =>
        d.copy(endpoint = d.endpoint.map(remapEndpoint))
      case a: TargetSettings.Alternator =>
        a.copy(alternatorEndpoint = remapEndpoint(a.alternatorEndpoint))
      case p: TargetSettings.Parquet =>
        p.copy(path = remapContainerPath(p.path))
      case s: TargetSettings.DynamoDBS3Export =>
        s.copy(path = remapContainerPath(s.path))
    }
    val newSavepoints = config.savepoints.copy(path = remapContainerPath(config.savepoints.path))
    config.copy(source = newSource, target = newTarget, savepoints = newSavepoints)
  }

  private def remapEndpoint(e: DynamoDBEndpoint): DynamoDBEndpoint =
    dynamoEndpointMap.get((e.host, e.port)) match {
      case Some((newHost, newPort)) => DynamoDBEndpoint(newHost, newPort)
      case None                     => e
    }

  /** Remap container paths (/app/parquet/..., /app/savepoints/...) to local test paths. */
  private def remapContainerPath(path: String): String =
    if (path.startsWith("/app/parquet"))
      path.replaceFirst("^/app/parquet", "docker/parquet")
    else if (path.startsWith("/app/savepoints"))
      path.replaceFirst("^/app/savepoints", "docker/spark-master")
    else if (path.startsWith("/app/spark-master"))
      path.replaceFirst("^/app/spark-master", "docker/spark-master")
    else
      path

  /** Run a migration with additional Spark SQL configs set for the duration of the job.
    *
    * The extra configs are set before the migration and restored to their original values
    * afterwards. This is useful for tests that need non-default Spark settings (e.g. partition
    * sizes).
    */
  def successfullyPerformMigrationWithConfigs(
    migratorConfigFile: String,
    extraConfigs: Map[String, String]
  ): Unit = {
    val originals = extraConfigs.map { case (k, _) =>
      k -> scala.util.Try(spark.conf.get(k)).toOption
    }
    try {
      extraConfigs.foreach { case (k, v) => spark.conf.set(k, v) }
      successfullyPerformMigration(migratorConfigFile)
    } finally
      originals.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None)    => spark.conf.unset(k)
      }
  }

}
