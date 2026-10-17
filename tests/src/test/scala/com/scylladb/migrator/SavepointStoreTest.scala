package com.scylladb.migrator

import com.scylladb.migrator.config.{
  Credentials,
  MigratorConfig,
  SavepointTarget,
  Savepoints,
  SourceSettings,
  TargetSettings
}

import com.datastax.oss.driver.api.core.{ ConsistencyLevel, CqlSession, DefaultConsistencyLevel }
import com.datastax.oss.driver.api.core.cql.{ BoundStatement, PreparedStatement, ResultSet, Row }
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import java.lang.reflect.{ InvocationHandler, Method, Proxy }

class SavepointStoreTest extends munit.FunSuite {

  Seq(
    ("ks", "savepoints", "ks.savepoints"),
    ("MyKs", "MySavepoints", "myks.mysavepoints"),
    ("my-keyspace", "my\"table", "\"my-keyspace\".\"my\"\"table\""),
    ("\"MyKs\"", "\"MySavepoints\"", "\"MyKs\".\"MySavepoints\"")
  ).foreach { case (keyspace, table, expectedReference) =>
    test(
      s"metadata queries use ${expectedReference} and LOCAL_QUORUM independently of bulk consistency"
    ) {
      val captured = scala.collection.mutable.ArrayBuffer.empty[(String, ConsistencyLevel)]
      def proxy[A](iface: Class[A])(handler: (Method, Array[AnyRef]) => AnyRef): A =
        Proxy
          .newProxyInstance(
            iface.getClassLoader,
            Array(iface),
            new InvocationHandler {
              override def invoke(instance: Any, method: Method, args: Array[AnyRef]): AnyRef =
                handler(method, Option(args).getOrElse(Array.empty[AnyRef]))
            }
          )
          .asInstanceOf[A]
      def prepared(query: String): PreparedStatement =
        proxy(classOf[PreparedStatement]) { (method, _) =>
          method.getName match {
            case "bind"     => bound(query, null)
            case "getQuery" => query
            case name       => fail(s"Unexpected prepared statement method: ${name}")
          }
        }
      def bound(query: String, consistency: ConsistencyLevel): BoundStatement =
        proxy(classOf[BoundStatement]) { (method, args) =>
          method.getName match {
            // Driver statements are immutable: the executed object must be the returned value.
            case "setConsistencyLevel"  => bound(query, args(0).asInstanceOf[ConsistencyLevel])
            case "getConsistencyLevel"  => consistency
            case "getPreparedStatement" => prepared(query)
            case name                   => fail(s"Unexpected bound statement method: ${name}")
          }
        }
      val emptyResult = proxy(classOf[ResultSet]) { (method, _) =>
        method.getName match {
          case "iterator" => java.util.Collections.emptyIterator[Row]()
          case name       => fail(s"Unexpected result set method: ${name}")
        }
      }
      val session = proxy(classOf[CqlSession]) { (method, args) =>
        method.getName match {
          case "prepare" => prepared(args(0).asInstanceOf[String])
          case "execute" =>
            val statement = args(0).asInstanceOf[BoundStatement]
            captured += statement.getPreparedStatement.getQuery -> statement.getConsistencyLevel
            emptyResult
          case name => fail(s"Unexpected session method: ${name}")
        }
      }
      val target = tableConfig(Set.empty).target
        .asInstanceOf[TargetSettings.Scylla]
        .copy(consistencyLevel = "ONE")
      val connector =
        new CassandraConnector(Connectors.buildTargetConf(new SparkConf(false), target)) {
          override def withSessionDo[T](action: CqlSession => T): T = action(session)
        }
      val store = new ScyllaTargetSavepointStore(connector, keyspace, table, "job", "identity", 1)
      store.save(SavepointRecord(SavepointCoordinates(1000L, 1L), "test", "payload"))
      assertEquals(store.seedState(), None)
      intercept[IllegalStateException](
        store.latestConfig()
      ) // Empty read still executes the statement.
      assertEquals(captured.size, 3)
      assert(captured(0)._1.startsWith(s"INSERT INTO ${expectedReference} ("))
      assert(
        captured(1)._1.startsWith(
          s"SELECT epoch_millis, sequence, config_sha256 FROM ${expectedReference} WHERE"
        )
      )
      assert(
        captured(2)._1.startsWith(
          s"SELECT epoch_millis, sequence, config_sha256, config_yaml FROM ${expectedReference} WHERE"
        )
      )
      captured.foreach { case (_, consistency) =>
        assertEquals(consistency, DefaultConsistencyLevel.LOCAL_QUORUM: ConsistencyLevel)
      }
    }

  }

  test("target store newestValidConfig returns newest valid savepoint") {
    val older = tableConfig(skipFiles = Set("older")).render
    val newer = tableConfig(skipFiles = Set("newer")).render

    val selected = ScyllaTargetSavepointStore.newestValidConfig(
      rows = newestFirst(
        ScyllaTargetSavepointStore.StoredSavepoint(1000L, 1L, older),
        ScyllaTargetSavepointStore.StoredSavepoint(1000L, 2L, newer)
      ),
      expectedConfigSha256 = SavepointStore.configIdentitySha256(tableConfig(Set.empty)),
      warnInvalidRow       = (_, _) => ()
    )

    assertEquals(
      selected.flatMap(_.skipParquetFiles),
      Some(Set("newer"))
    )
  }

  test("target store newestValidConfig skips corrupt latest rows") {
    val warnings = scala.collection.mutable.ArrayBuffer.empty[(Long, Long)]
    val valid = tableConfig(skipFiles = Set("valid")).render

    val selected = ScyllaTargetSavepointStore.newestValidConfig(
      rows = newestFirst(
        ScyllaTargetSavepointStore.StoredSavepoint(3000L, 1L, "not: [valid"),
        ScyllaTargetSavepointStore.StoredSavepoint(2000L, 1L, valid)
      ),
      expectedConfigSha256 = SavepointStore.configIdentitySha256(tableConfig(Set.empty)),
      warnInvalidRow       = (row, _) => warnings += ((row.epochMillis, row.sequence))
    )

    assertEquals(selected.flatMap(_.skipParquetFiles), Some(Set("valid")))
    assertEquals(warnings.toSeq, Seq((3000L, 1L)))
  }

  test("target store newestValidConfig ignores hostile future coordinates") {
    val warnings = scala.collection.mutable.ArrayBuffer.empty[(Long, Long)]
    val real = tableConfig(skipFiles = Set("real")).render
    val hostile = tableConfig(skipFiles = Set("hostile")).render

    val selected = ScyllaTargetSavepointStore.newestValidConfig(
      rows = newestFirst(
        ScyllaTargetSavepointStore.StoredSavepoint(1000L, 1L, real),
        ScyllaTargetSavepointStore.StoredSavepoint(
          SavepointsManager.MaxReasonableSeedValue,
          1L,
          hostile
        )
      ),
      expectedConfigSha256 = SavepointStore.configIdentitySha256(tableConfig(Set.empty)),
      warnInvalidRow       = (row, _) => warnings += ((row.epochMillis, row.sequence))
    )

    assertEquals(
      selected.flatMap(_.skipParquetFiles),
      Some(Set("real")),
      "target-backed resume must not let an unreasonable future row dominate ordering"
    )
    assertEquals(warnings.toSeq, Seq((SavepointsManager.MaxReasonableSeedValue, 1L)))
  }

  test("target store newestValidConfig skips max-counter boundary rows") {
    val warnings = scala.collection.mutable.ArrayBuffer.empty[(Long, Long)]
    val real = tableConfig(skipFiles = Set("real")).render
    val boundary = tableConfig(skipFiles = Set("boundary")).render

    val selected = ScyllaTargetSavepointStore.newestValidConfig(
      rows = newestFirst(
        ScyllaTargetSavepointStore.StoredSavepoint(1000L, 1L, real),
        ScyllaTargetSavepointStore.StoredSavepoint(
          SavepointsManager.MaxReasonableSeedValue - 1L,
          SavepointsManager.MaxSavepointSequenceValue,
          boundary
        )
      ),
      expectedConfigSha256 = SavepointStore.configIdentitySha256(tableConfig(Set.empty)),
      warnInvalidRow       = (row, _) => warnings += ((row.epochMillis, row.sequence))
    )

    assertEquals(
      selected.flatMap(_.skipParquetFiles),
      Some(Set("real")),
      "target-backed resume must not select a row whose next savepoint would roll over to an unresumable coordinate"
    )
    assertEquals(
      warnings.toSeq,
      Seq(
        (
          SavepointsManager.MaxReasonableSeedValue - 1L,
          SavepointsManager.MaxSavepointSequenceValue
        )
      )
    )
  }

  test("target store newestCoordinates skips max-counter boundary seeds") {
    val warnings = scala.collection.mutable.ArrayBuffer.empty[(Long, Long)]

    val selected = ScyllaTargetSavepointStore.newestCoordinates(
      rows = Seq(
        ScyllaTargetSavepointStore.StoredSavepointCoordinates(
          epochMillis  = 1000L,
          sequence     = 1L,
          configSha256 = Some("matching")
        ),
        ScyllaTargetSavepointStore.StoredSavepointCoordinates(
          epochMillis  = SavepointsManager.MaxReasonableSeedValue - 1L,
          sequence     = SavepointsManager.MaxSavepointSequenceValue,
          configSha256 = Some("matching")
        )
      ).sortBy(row => (row.epochMillis, row.sequence)).reverseIterator,
      expectedConfigSha256 = "matching",
      warnInvalidRow       = (row, _) => warnings += ((row.epochMillis, row.sequence))
    )

    assertEquals(
      selected,
      Some(SavepointCoordinates(1000L, 1L)),
      "target-backed seed state must ignore a coordinate whose rollover would be rejected by resume lookup"
    )
    assertEquals(
      warnings.toSeq,
      Seq(
        (
          SavepointsManager.MaxReasonableSeedValue - 1L,
          SavepointsManager.MaxSavepointSequenceValue
        )
      )
    )
  }

  test("target store newestValidConfig skips rows from different config identities") {
    val requested = tableConfig(sourcePath = "s3a://bucket/requested", skipFiles = Set("requested"))
    val unrelated = tableConfig(sourcePath = "s3a://bucket/unrelated", skipFiles = Set("unrelated"))
    val requestedSha = SavepointStore.configIdentitySha256(requested)
    val unrelatedSha = SavepointStore.configIdentitySha256(unrelated)

    val selected = ScyllaTargetSavepointStore.newestValidConfig(
      rows = newestFirst(
        ScyllaTargetSavepointStore.StoredSavepoint(
          3000L,
          1L,
          unrelated.render,
          Some(requestedSha)
        ),
        ScyllaTargetSavepointStore.StoredSavepoint(
          2000L,
          1L,
          unrelated.render,
          Some(unrelatedSha)
        ),
        ScyllaTargetSavepointStore.StoredSavepoint(
          1000L,
          1L,
          requested.render,
          Some(requestedSha)
        )
      ),
      expectedConfigSha256 = requestedSha,
      warnInvalidRow       = (_, _) => ()
    )

    assertEquals(
      selected.map(SavepointStore.configIdentitySha256),
      Some(SavepointStore.configIdentitySha256(requested)),
      "target-backed resume must not use a newer row that belongs to a different migration identity"
    )
  }

  test("target store newestCoordinates skips rows from different config identities") {
    val requested = tableConfig(sourcePath = "s3a://bucket/requested", skipFiles = Set("requested"))
    val unrelated = tableConfig(sourcePath = "s3a://bucket/unrelated", skipFiles = Set("unrelated"))
    val requestedSha = SavepointStore.configIdentitySha256(requested)
    val unrelatedSha = SavepointStore.configIdentitySha256(unrelated)

    val selected = ScyllaTargetSavepointStore.newestCoordinates(
      rows = Seq(
        ScyllaTargetSavepointStore.StoredSavepointCoordinates(
          epochMillis  = 3000L,
          sequence     = 1L,
          configSha256 = Some(unrelatedSha)
        ),
        ScyllaTargetSavepointStore.StoredSavepointCoordinates(
          epochMillis  = 2000L,
          sequence     = 1L,
          configSha256 = Some(requestedSha)
        )
      ),
      expectedConfigSha256 = requestedSha,
      warnInvalidRow       = (_, _) => ()
    )

    assertEquals(
      selected,
      Some(SavepointCoordinates(2000L, 1L)),
      "target-backed seed state must ignore coordinates from another migration identity"
    )
  }

  private def newestFirst(rows: ScyllaTargetSavepointStore.StoredSavepoint*) =
    rows
      .map(row =>
        if (row.configSha256.isDefined) row
        else
          row.copy(
            configSha256 = Some(SavepointStore.configIdentitySha256(tableConfig(Set.empty)))
          )
      )
      .sortBy(row => (row.epochMillis, row.sequence))
      .reverseIterator

  test("table selection stops consuming history after the first valid matching payload") {
    val config = tableConfig(Set("latest"))
    val identity = SavepointStore.configIdentitySha256(config)
    val rows = Iterator(
      ScyllaTargetSavepointStore.StoredSavepoint(3000L, 1L, "broken: [", Some(identity)),
      ScyllaTargetSavepointStore.StoredSavepoint(
        2000L,
        1L,
        SavepointStore.renderSavepoint(config),
        Some(identity)
      )
    ) ++ Iterator.continually(throw new AssertionError("consumed older history after success"))
    val warnings = scala.collection.mutable.ArrayBuffer.empty[Throwable]
    val selected =
      ScyllaTargetSavepointStore.newestValidConfig(rows, identity, (_, error) => warnings += error)
    assertEquals(selected.flatMap(_.skipParquetFiles), Some(Set("latest")))
    assertEquals(warnings.size, 1)
  }

  test("seed selection stops after the first safe matching coordinate") {
    val rows = Iterator(
      ScyllaTargetSavepointStore
        .StoredSavepointCoordinates(SavepointsManager.MaxReasonableSeedValue, 1L, Some("matching")),
      ScyllaTargetSavepointStore.StoredSavepointCoordinates(3000L, 1L, Some("unrelated")),
      ScyllaTargetSavepointStore.StoredSavepointCoordinates(2000L, 1L, Some("matching"))
    ) ++ Iterator.continually(throw new AssertionError("consumed older seed history"))
    val warnings = scala.collection.mutable.ArrayBuffer.empty[Throwable]
    val selected = ScyllaTargetSavepointStore.newestCoordinates(
      rows,
      "matching",
      (_, error) => warnings += error
    )
    assertEquals(selected, Some(SavepointCoordinates(2000L, 1L)))
    assertEquals(warnings.size, 2)
  }

  private def tableConfig(
    skipFiles: Set[String],
    sourcePath: String = "s3a://bucket/data"
  ): MigratorConfig = {
    val base = config(skipFiles, sourcePath)
    base.copy(savepoints = base.savepoints.copy(target = Some(SavepointTarget.TargetTable())))
  }

  test("redacted table payloads verify WHERE identity and reject misleading row metadata") {
    val source = SourceSettings.Cassandra(
      host               = "localhost",
      port               = 9042,
      localDC            = None,
      credentials        = Some(Credentials("reader", "old-password")),
      sslOptions         = None,
      keyspace           = "ks",
      table              = "source",
      splitCount         = None,
      connections        = None,
      fetchSize          = 1000,
      preserveTimestamps = false,
      where              = Some("tenant = 'private-tenant'"),
      consistencyLevel   = "LOCAL_QUORUM"
    )
    val saved = tableConfig(Set("saved")).copy(source = source)
    val live =
      saved.copy(source = source.copy(credentials = Some(Credentials("reader", "new-password"))))
    val expected = SavepointStore.configIdentitySha256(live)
    val payload = SavepointStore.renderSavepoint(saved)
    assert(!payload.contains("private-tenant"))
    assert(!payload.contains("old-password"))
    assertEquals(
      SavepointStore.payloadIdentitySha256(payload, MigratorConfig.loadFromString(payload)),
      expected
    )

    val differentWhere = SavepointStore.renderSavepoint(
      saved.copy(source = source.copy(where = Some("tenant = 'other'")))
    )
    val unrelated =
      SavepointStore.renderSavepoint(saved.copy(source = source.copy(table = "other")))
    val missingDigest = saved.renderRedacted
    val malformedDigest =
      payload.replaceAll("(?m)^_savepointWhereSha256:.*$", "_savepointWhereSha256: invalid")
    val badPayloads = Seq(differentWhere, unrelated, missingDigest, malformedDigest)
    val warnings = scala.collection.mutable.ArrayBuffer.empty[Throwable]
    val selected = ScyllaTargetSavepointStore.newestValidConfig(
      (badPayloads.zipWithIndex.map { case (yaml, index) =>
        ScyllaTargetSavepointStore.StoredSavepoint(2000L + index, 1L, yaml, Some(expected))
      } :+ ScyllaTargetSavepointStore.StoredSavepoint(1000L, 1L, payload, Some(expected)))
        .sortBy(row => (row.epochMillis, row.sequence))
        .reverseIterator,
      expected,
      (_, error) => warnings += error
    )
    assertEquals(warnings.size, badPayloads.size)
    assertEquals(selected.flatMap(_.skipParquetFiles), Some(Set("saved")))
    assertEquals(SavepointStore.payloadIdentitySha256(saved.render, saved), expected)
    assertEquals(SavepointsResume.mergeSkipState(live, selected.get).source, live.source)
  }

  private def config(
    skipFiles: Set[String],
    sourcePath: String = "s3a://bucket/data",
    savepointsPath: String = "/tmp/savepoints"
  ): MigratorConfig =
    MigratorConfig(
      source = SourceSettings.Parquet(
        path        = sourcePath,
        credentials = None,
        region      = None,
        endpoint    = None
      ),
      target = TargetSettings.Scylla(
        host                          = "scylla.example.com",
        port                          = 9042,
        localDC                       = Some("dc1"),
        credentials                   = None,
        sslOptions                    = None,
        keyspace                      = "ks",
        table                         = "tbl",
        connections                   = None,
        stripTrailingZerosForDecimals = false,
        writeTTLInS                   = None,
        writeWritetimestampInuS       = None,
        consistencyLevel              = "LOCAL_QUORUM"
      ),
      renames          = None,
      savepoints       = Savepoints(intervalSeconds = 300, path = savepointsPath),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = Some(skipFiles),
      validation       = None
    )
}
