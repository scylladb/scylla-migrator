package com.scylladb.migrator.readers

import com.scylladb.migrator.{ Integration, SparkUtils }
import com.scylladb.migrator.config.{ Credentials, SourceSettings }
import org.apache.spark.sql.functions.{ col, date_format }
import org.junit.experimental.categories.Category

import java.sql.DriverManager
import java.util.TimeZone
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.util.Using

@Category(Array(classOf[Integration]))
class MySQLReaderIntegrationTest extends munit.FunSuite {

  override val munitTimeout: Duration = 180.seconds

  private val rootJdbcUrl =
    "jdbc:mysql://localhost:3308/testdb?zeroDateTimeBehavior=EXCEPTION&tinyInt1IsBit=false&useSSL=false"

  Class.forName("com.mysql.cj.jdbc.Driver")

  private def withRootConnection[A](f: java.sql.Connection => A): A =
    Using.resource(DriverManager.getConnection(rootJdbcUrl, "root", "root"))(f)

  private def executeRoot(sql: String): Unit =
    withRootConnection { connection =>
      Using.resource(connection.createStatement()) { statement =>
        statement.execute(sql)
        ()
      }
    }

  test(
    "partitioned timestamp reads stay stable when JVM, Spark, and MySQL session time zones differ"
  ) {
    val table = "users_tz_partition"
    val originalTimeZone = TimeZone.getDefault

    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))

      executeRoot("SET GLOBAL time_zone = '+05:00'")
      withRootConnection { connection =>
        Using.resource(connection.createStatement()) { statement =>
          statement.execute("SET SESSION time_zone = '+05:00'")
          statement.execute(s"DROP TABLE IF EXISTS $table")
          statement.execute(
            s"""CREATE TABLE $table (
               |  id BIGINT PRIMARY KEY,
               |  created_at TIMESTAMP NOT NULL
               |)""".stripMargin
          )
          statement.execute(
            s"""INSERT INTO $table (id, created_at) VALUES
               |  (1, '2024-01-01 00:30:00'),
               |  (2, '2024-01-01 12:00:00'),
               |  (3, '2024-01-01 23:30:00')""".stripMargin
          )
        }
      }

      val source = SourceSettings.MySQL(
        host                 = "localhost",
        port                 = 3308,
        database             = "testdb",
        table                = table,
        credentials          = Credentials("migrator", "migrator"),
        primaryKey           = None,
        partitionColumn      = Some("created_at"),
        numPartitions        = Some(2),
        lowerBound           = Some(SourceSettings.MySQL.PartitionBound("2024-01-01 00:00:00")),
        upperBound           = Some(SourceSettings.MySQL.PartitionBound("2024-01-02 00:00:00")),
        zeroDateTimeBehavior = SourceSettings.MySQL.ZeroDateTimeBehavior.Exception,
        fetchSize            = 1000,
        where                = None,
        connectionProperties = None
      )

      val rows = SparkUtils.withSparkSession(Map("spark.sql.session.timeZone" -> "UTC")) { spark =>
        MySQL
          .readDataframe(spark, source)
          .dataFrame
          .select(
            col("id"),
            date_format(col("created_at"), "yyyy-MM-dd HH:mm:ss").as("created_at_utc")
          )
          .orderBy("id")
          .collect()
          .toList
          .map(row => row.getLong(0) -> row.getString(1))
      }

      assertEquals(
        rows,
        List(
          1L -> "2023-12-31 19:30:00",
          2L -> "2024-01-01 07:00:00",
          3L -> "2024-01-01 18:30:00"
        )
      )
    } finally {
      executeRoot(s"DROP TABLE IF EXISTS $table")
      executeRoot("SET GLOBAL time_zone = 'SYSTEM'")
      TimeZone.setDefault(originalTimeZone)
    }
  }
}
