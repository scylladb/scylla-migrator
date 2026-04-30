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
class MySQLMigrationIntegrationTest extends munit.FunSuite {

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

  private def withTables(
    mysqlTable: String,
    mysqlCreate: String,
    scyllaTable: String,
    scyllaCreate: String
  )(body: => Unit): Unit = {
    targetScylla().execute(s"DROP TABLE IF EXISTS $keyspace.$scyllaTable")
    executeMySql(s"DROP TABLE IF EXISTS $mysqlTable")
    executeMySql(mysqlCreate)
    targetScylla().execute(scyllaCreate)
    awaitScyllaTable(scyllaTable)
    try body
    finally {
      targetScylla().execute(s"DROP TABLE IF EXISTS $keyspace.$scyllaTable")
      executeMySql(s"DROP TABLE IF EXISTS $mysqlTable")
    }
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

  test("MySQL: numeric partitioning migrates rows from a real JDBC source") {
    withTables(
      mysqlTable = "users_numeric",
      mysqlCreate = """CREATE TABLE users_numeric (
                      |  id BIGINT PRIMARY KEY,
                      |  name VARCHAR(255) NOT NULL,
                      |  payload LONGTEXT NOT NULL
                      |)""".stripMargin,
      scyllaTable = "users_numeric",
      scyllaCreate = s"""CREATE TABLE $keyspace.users_numeric (
                        |  id bigint PRIMARY KEY,
                        |  name text,
                        |  payload text
                        |)""".stripMargin
    ) {
      executeMySql(
        """INSERT INTO users_numeric (id, name, payload) VALUES
          |  (1, 'alice', 'payload-a'),
          |  (2, 'bob', 'payload-b'),
          |  (3, 'carol', 'payload-c')
          |""".stripMargin
      )

      SparkUtils.successfullyPerformMigration("mysql-to-scylla-numeric.yaml")

      val rows = targetScylla()
        .execute(s"SELECT id, name, payload FROM $keyspace.users_numeric")
        .all()
        .asScala
        .map(row => (row.getLong("id"), row.getString("name"), row.getString("payload")))
        .sortBy(_._1)
        .toSeq

      assertEquals(
        rows,
        Seq(
          (1L, "alice", "payload-a"),
          (2L, "bob", "payload-b"),
          (3L, "carol", "payload-c")
        )
      )
    }
  }

  test("MySQL: validator passes on clean data and fails on a hashed-column mismatch") {
    withTables(
      mysqlTable = "users_numeric",
      mysqlCreate = """CREATE TABLE users_numeric (
                      |  id BIGINT PRIMARY KEY,
                      |  name VARCHAR(255) NOT NULL,
                      |  payload LONGTEXT NOT NULL
                      |)""".stripMargin,
      scyllaTable = "users_numeric",
      scyllaCreate = s"""CREATE TABLE $keyspace.users_numeric (
                        |  id bigint PRIMARY KEY,
                        |  name text,
                        |  payload text
                        |)""".stripMargin
    ) {
      executeMySql(
        """INSERT INTO users_numeric (id, name, payload) VALUES
          |  (11, 'delta', 'payload-d'),
          |  (12, 'echo', 'payload-e')
          |""".stripMargin
      )

      SparkUtils.successfullyPerformMigration("mysql-to-scylla-numeric.yaml")
      assertEquals(SparkUtils.performValidation("mysql-to-scylla-numeric.yaml"), 0)

      targetScylla().execute(
        s"UPDATE $keyspace.users_numeric SET payload = 'tampered' WHERE id = 12"
      )

      assertEquals(SparkUtils.performValidation("mysql-to-scylla-numeric.yaml"), 1)
    }
  }

  test("MySQL: temporal TIMESTAMP partitioning migrates rows from a real JDBC source") {
    withTables(
      mysqlTable = "users",
      mysqlCreate = """CREATE TABLE users (
                      |  id BIGINT PRIMARY KEY,
                      |  name VARCHAR(255) NOT NULL,
                      |  created_at TIMESTAMP NOT NULL,
                      |  payload LONGTEXT NOT NULL
                      |)""".stripMargin,
      scyllaTable = "users",
      scyllaCreate = s"""CREATE TABLE $keyspace.users (
                        |  id bigint PRIMARY KEY,
                        |  name text,
                        |  created_at timestamp,
                        |  payload text
                        |)""".stripMargin
    ) {
      executeMySql(
        """INSERT INTO users (id, name, created_at, payload) VALUES
          |  (21, 'jan', '2024-01-01 00:00:00', 'payload-jan'),
          |  (22, 'feb', '2024-01-10 12:00:00', 'payload-feb'),
          |  (23, 'mar', '2024-01-20 23:59:59', 'payload-mar')
          |""".stripMargin
      )

      SparkUtils.successfullyPerformMigration("mysql-to-scylla-basic.yaml")

      val rows = targetScylla()
        .execute(s"SELECT id, created_at, payload FROM $keyspace.users")
        .all()
        .asScala
        .map(row =>
          (row.getLong("id"), row.getInstant("created_at") != null, row.getString("payload"))
        )
        .sortBy(_._1)
        .toSeq

      assertEquals(
        rows,
        Seq(
          (21L, true, "payload-jan"),
          (22L, true, "payload-feb"),
          (23L, true, "payload-mar")
        )
      )
    }
  }

  test(
    "MySQL: filtered validation ignores unrelated target rows and validates only the filtered slice"
  ) {
    withTables(
      mysqlTable = "users_filtered",
      mysqlCreate = """CREATE TABLE users_filtered (
                      |  id BIGINT PRIMARY KEY,
                      |  name VARCHAR(255) NOT NULL,
                      |  created_at TIMESTAMP NOT NULL,
                      |  payload LONGTEXT NOT NULL
                      |)""".stripMargin,
      scyllaTable = "users_filtered",
      scyllaCreate = s"""CREATE TABLE $keyspace.users_filtered (
                        |  id bigint PRIMARY KEY,
                        |  name text,
                        |  created_at timestamp,
                        |  payload text
                        |)""".stripMargin
    ) {
      executeMySql(
        """INSERT INTO users_filtered (id, name, created_at, payload) VALUES
          |  (31, 'jan', '2024-01-01 00:00:00', 'payload-jan'),
          |  (32, 'feb', '2024-01-10 12:00:00', 'payload-feb'),
          |  (33, 'mar', '2024-01-20 23:59:59', 'payload-mar')
          |""".stripMargin
      )

      SparkUtils.successfullyPerformMigration("mysql-to-scylla-filtered.yaml")
      targetScylla().execute(
        s"""INSERT INTO $keyspace.users_filtered (id, name, created_at, payload)
           |VALUES (99, 'extra', '2024-01-05T00:00:00Z', 'unrelated-target-row')""".stripMargin
      )

      assertEquals(SparkUtils.performValidation("mysql-to-scylla-filtered.yaml"), 0)
    }
  }
}
