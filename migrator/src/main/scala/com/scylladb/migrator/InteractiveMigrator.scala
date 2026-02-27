package com.scylladb.migrator

import com.scylladb.migrator.config.MigratorConfig
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession

/** An interactive Spark entry point that keeps a single SparkSession alive across multiple
  * migration and validation jobs. This eliminates JVM and Spark initialization overhead when
  * running multiple test jobs sequentially.
  *
  * Protocol (stdin/stdout):
  *   - Prints "READY" when the SparkSession is initialized
  *   - Reads commands from stdin, one per line:
  *     - `<configPath>` or `MIGRATE:<configPath>`: run a migration, responds with "OK" or
  *       "FAIL:<message>"
  *     - `VALIDATE:<configPath>`: run validation, responds with "OK:0" (no failures) or "OK:1"
  *       (failures found) or "FAIL:<message>"
  *     - `EXIT` or empty line: shut down
  */
object InteractiveMigrator {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("scylla-migrator-interactive")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.WARN)
    Migrator.log.setLevel(Level.INFO)
    Logger.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(Level.WARN)
    Logger.getLogger("com.datastax.spark.connector.cql.CassandraConnector").setLevel(Level.WARN)

    System.out.println("READY")
    System.out.flush()

    val scanner = new java.util.Scanner(System.in)
    try
      while (scanner.hasNextLine) {
        val line = scanner.nextLine().trim
        if (line == "EXIT" || line.isEmpty) return

        try
          if (line.startsWith("VALIDATE:")) {
            val configFile = line.substring("VALIDATE:".length)
            val config = MigratorConfig.loadFrom(configFile)
            val failures = Validator.runValidation(config)
            if (failures.isEmpty) System.out.println("OK:0")
            else System.out.println("OK:1")
          } else {
            val configFile =
              if (line.startsWith("MIGRATE:")) line.substring("MIGRATE:".length) else line
            val config = MigratorConfig.loadFrom(configFile)
            Migrator.runMigration(config)
            System.out.println("OK")
          }
        catch {
          case e: Exception =>
            Migrator.log.error("Job failed", e)
            System.out.println(s"FAIL:${e.getMessage.replace('\n', ' ')}")
        }
        System.out.flush()
      }
    finally
      spark.stop()
  }

}
