package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.scylladb.migrator.{ Integration, SparkUtils }
import org.junit.experimental.categories.Category

import java.net.InetSocketAddress
import java.sql.{ Connection, DriverManager }
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.jdk.CollectionConverters._
import scala.util.Using

@Category(Array(classOf[Integration]))
class MySQLCopyMissingRowsTest extends munit.FunSuite {

  override val munitTimeout: Duration = 180.seconds

  private val keyspace = "test"
  private val mysqlJdbcUrl =
    "jdbc:mysql://localhost:3308/testdb?zeroDateTimeBehavior=EXCEPTION&tinyInt1IsBit=false&useSSL=false"

  val sourceMySQL: Fixture[Connection] = new Fixture[Connection]("sourceMySQL") {
    private var connection: Connection = null

    def apply(): Connection = connection

    override def beforeAll(): Unit = {
      Class.forName("com.mysql.cj.jdbc.Driver")
      connection = DriverManager.getConnection(mysqlJdbcUrl, "migrator", "migrator")
    }

    override def afterAll(): Unit =
      if (connection != null) connection.close()
  }

  val targetScylla: Fixture[CqlSession] = new Fixture[CqlSession]("targetScylla") {
    private var session: CqlSession = null

    def apply(): CqlSession = session

    override def beforeAll(): Unit = {
      session = CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress("localhost", 9042))
        .withLocalDatacenter("datacenter1")
        .withAuthCredentials("dummy", "dummy")
        .build()
      session.execute(
        s"CREATE KEYSPACE IF NOT EXISTS $keyspace " +
          "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
      )
    }

    override def afterAll(): Unit =
      if (session != null) session.close()
  }

  override def munitFixtures: Seq[Fixture[_]] = Seq(sourceMySQL, targetScylla)

  private def executeMySql(sql: String): Unit =
    Using.resource(sourceMySQL().createStatement()) { statement =>
      statement.executeUpdate(sql)
      ()
    }

  private def awaitScyllaTable(table: String): Unit = {
    val deadline = System.nanoTime() + 30.seconds.toNanos
    var visible = false

    while (!visible && System.nanoTime() < deadline) {
      visible = targetScylla()
        .execute(
          s"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '$keyspace' AND table_name = '$table'"
        )
        .one() != null
      if (!visible) Thread.sleep(250)
    }

    if (!visible) fail(s"Timed out waiting for Scylla table $keyspace.$table to become visible")
  }

  test("MySQL: copyMissingRows copies missing rows to target") {
    val mysqlTable = "users"
    val scyllaTable = "users"
    targetScylla().execute(s"DROP TABLE IF EXISTS $keyspace.$scyllaTable")
    executeMySql(s"DROP TABLE IF EXISTS $mysqlTable")
    executeMySql(
      """CREATE TABLE users (
        |  id BIGINT PRIMARY KEY,
        |  name VARCHAR(255) NOT NULL,
        |  created_at TIMESTAMP NOT NULL,
        |  payload LONGTEXT NOT NULL
        |)""".stripMargin
    )
    targetScylla().execute(
      s"""CREATE TABLE $keyspace.$scyllaTable (
         |  id bigint PRIMARY KEY,
         |  name text,
         |  created_at timestamp,
         |  payload text
         |)""".stripMargin
    )
    awaitScyllaTable(scyllaTable)

    try {
      executeMySql(
        """INSERT INTO users (id, name, created_at, payload) VALUES
          |  (100, 'alice', '2024-01-15 10:00:00', 'data-a'),
          |  (101, 'bob',   '2024-01-15 11:00:00', 'data-b'),
          |  (102, 'carol', '2024-01-15 12:00:00', 'data-c')
          |""".stripMargin
      )

      SparkUtils.successfullyPerformMigration("mysql-to-scylla-basic.yaml")
      assertEquals(
        SparkUtils.performValidation("mysql-to-scylla-basic.yaml"),
        0,
        "Initial validation should pass"
      )

      targetScylla().execute(s"DELETE FROM $keyspace.$scyllaTable WHERE id = 101")

      assertEquals(
        SparkUtils.performValidation("mysql-to-scylla-copy-missing-rows.yaml"),
        1,
        "Should detect 1 missing row"
      )

      assertEquals(
        SparkUtils.performValidation("mysql-to-scylla-basic.yaml"),
        0,
        "Re-validation should pass after missing row was copied"
      )

      targetScylla().execute(
        s"UPDATE $keyspace.$scyllaTable SET payload = 'tampered' WHERE id = 100"
      )

      assertEquals(
        SparkUtils.performValidation("mysql-to-scylla-copy-missing-rows.yaml"),
        1,
        "Should detect 1 mismatched row but not overwrite it"
      )

      val tamperedRow = targetScylla()
        .execute(s"SELECT payload FROM $keyspace.$scyllaTable WHERE id = 100")
        .one()
      assertEquals(
        tamperedRow.getString("payload"),
        "tampered",
        "copyMissingRows must not overwrite rows with differing values"
      )

      val rows = targetScylla()
        .execute(s"SELECT id, name, payload FROM $keyspace.$scyllaTable")
        .all()
        .asScala
        .map(row => (row.getLong("id"), row.getString("name"), row.getString("payload")))
        .sortBy(_._1)
        .toSeq

      assertEquals(
        rows,
        Seq(
          (100L, "alice", "tampered"),
          (101L, "bob", "data-b"),
          (102L, "carol", "data-c")
        ),
        "All three rows should exist in target after copy"
      )
    } finally {
      targetScylla().execute(s"DROP TABLE IF EXISTS $keyspace.$scyllaTable")
      executeMySql(s"DROP TABLE IF EXISTS $mysqlTable")
    }
  }
}
