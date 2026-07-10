package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row
import com.scylladb.migrator.{ Integration, TestFileUtils }
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import org.junit.experimental.categories.Category

import java.net.InetSocketAddress
import java.nio.file.{ Files, Paths }
import scala.jdk.CollectionConverters._

/** End-to-end coverage for per-element TTL/WRITETIME preservation of non-frozen collections across
  * the Parquet intermediate format: Cassandra 5.0 -> Parquet -> Cassandra 5.0.
  *
  * The export writes each non-frozen collection's per-element metadata as `__migrator_meta_*` array
  * sidecars; the restore re-hydrates them into collection-append passes. Cassandra 5.0 is used for
  * both ends because ScyllaDB cannot read back `WRITETIME()`/`TTL()` of non-frozen collections for
  * verification.
  */
@Category(Array(classOf[Integration]))
class CollectionTimestampParquetRoundTripTest extends munit.FunSuite {

  private val keyspace = "test"
  private val sourceTbl = "collts_pq_src"
  private val targetTbl = "collts_pq_dst"
  private val exportConfig = "cassandra5-to-parquet-collection-timestamps.yaml"
  private val restoreConfig = "parquet-to-cassandra5-collection-timestamps.yaml"
  private val parquetDir = Paths.get("docker/parquet/collection-timestamps")

  private val cassandra5: Fixture[CqlSession] = new Fixture[CqlSession]("cassandra5") {
    private var session: CqlSession = null
    def apply(): CqlSession = session
    override def beforeAll(): Unit = {
      session = CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress("localhost", 9047))
        .withLocalDatacenter("datacenter1")
        .withAuthCredentials("dummy", "dummy")
        .build()
      session.execute(
        s"CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH replication = " +
          "{'class':'SimpleStrategy','replication_factor':1}"
      )
    }
    override def afterAll(): Unit = if (session != null) session.close()
  }

  override def munitFixtures: Seq[Fixture[_]] = Seq(cassandra5)

  private def createTable(session: CqlSession, table: String): Unit = {
    session.execute(s"DROP TABLE IF EXISTS ${keyspace}.${table}")
    session.execute(
      s"CREATE TABLE ${keyspace}.${table} " +
        "(id text PRIMARY KEY, name text, tags set<int>, attrs map<text,int>)"
    )
  }

  private case class CollectionMeta(
    tags: List[Int],
    tagsWt: List[Long],
    attrs: Map[String, Int],
    attrsWt: List[Long],
    attrsTtl: List[Integer],
    name: String,
    nameWt: Long
  )

  private def readMeta(session: CqlSession, table: String, id: String): CollectionMeta = {
    val row: Row = session
      .execute(
        s"SELECT name, WRITETIME(name) AS name_wt, " +
          s"tags, WRITETIME(tags) AS tags_wt, " +
          s"attrs, WRITETIME(attrs) AS attrs_wt, TTL(attrs) AS attrs_ttl " +
          s"FROM ${keyspace}.${table} WHERE id = '${id}'"
      )
      .one()
    assert(row != null, s"expected a row for id=${id} in ${table}")
    CollectionMeta(
      tags   = row.getSet("tags", classOf[Integer]).asScala.toList.map(_.intValue()),
      tagsWt = row.getList("tags_wt", classOf[java.lang.Long]).asScala.toList.map(_.longValue()),
      attrs = row
        .getMap("attrs", classOf[String], classOf[Integer])
        .asScala
        .toMap
        .map { case (k, v) => k -> v.intValue() },
      attrsWt  = row.getList("attrs_wt", classOf[java.lang.Long]).asScala.toList.map(_.longValue()),
      attrsTtl = row.getList("attrs_ttl", classOf[Integer]).asScala.toList,
      name     = row.getString("name"),
      nameWt   = row.getLong("name_wt")
    )
  }

  test("preserves per-element TTL/WRITETIME of non-frozen collections through Parquet") {
    val session = cassandra5()
    createTable(session, sourceTbl)
    createTable(session, targetTbl)
    TestFileUtils.deleteRecursive(parquetDir.toFile)

    val id = "r1"
    session.execute(
      s"INSERT INTO ${keyspace}.${sourceTbl} (id, name) VALUES ('${id}', 'alice') " +
        s"USING TIMESTAMP 1000000000000"
    )

    val tagWrites = Seq(
      50 -> 5000000000000L,
      10 -> 1000000000000L,
      30 -> 3000000000000L,
      20 -> 2000000000000L,
      40 -> 4000000000000L
    )
    tagWrites.foreach { case (v, wt) =>
      session.execute(
        s"UPDATE ${keyspace}.${sourceTbl} USING TIMESTAMP ${wt} SET tags = tags + {${v}} WHERE id='${id}'"
      )
    }

    val attrWrites = Seq(
      ("f", 6, 6000000000000L, Some(1000000)),
      ("a", 1, 1000000000000L, None),
      ("e", 5, 5000000000000L, Some(2000000)),
      ("b", 2, 2000000000000L, None),
      ("d", 4, 4000000000000L, Some(3000000)),
      ("c", 3, 3000000000000L, None)
    )
    attrWrites.foreach { case (k, v, wt, ttlOpt) =>
      val using = ttlOpt match {
        case Some(ttl) => s"USING TIMESTAMP ${wt} AND TTL ${ttl}"
        case None      => s"USING TIMESTAMP ${wt}"
      }
      session.execute(
        s"UPDATE ${keyspace}.${sourceTbl} ${using} SET attrs = attrs + {'${k}': ${v}} WHERE id='${id}'"
      )
    }

    // 1) Cassandra 5.0 -> Parquet (array metadata sidecars).
    successfullyPerformMigration(exportConfig)
    assert(
      Files.exists(parquetDir) && Files.list(parquetDir).iterator().asScala.nonEmpty,
      "Parquet output directory should contain files"
    )

    // 2) Parquet -> Cassandra 5.0 (collection-append replay).
    successfullyPerformMigration(restoreConfig)

    val src = readMeta(session, sourceTbl, id)
    val dst = readMeta(session, targetTbl, id)

    assertEquals(dst.name, src.name)
    assertEquals(dst.nameWt, src.nameWt, "scalar WRITETIME must survive the Parquet round-trip")

    assertEquals(dst.tags.sorted, src.tags.sorted, "set elements must match")
    assertEquals(dst.tagsWt, src.tagsWt, "set element WRITETIMEs must be preserved")

    assertEquals(dst.attrs, src.attrs, "map entries must match")
    assertEquals(dst.attrsWt, src.attrsWt, "map element WRITETIMEs must be preserved")

    assertEquals(dst.attrsTtl.length, src.attrsTtl.length)
    dst.attrsTtl.zip(src.attrsTtl).zipWithIndex.foreach { case ((dTtl, sTtl), i) =>
      if (sTtl == null) assert(dTtl == null, s"attrs TTL[$i] expected null, got ${dTtl}")
      else {
        assert(dTtl != null, s"attrs TTL[$i] expected non-null")
        val diff = math.abs(sTtl.intValue() - dTtl.intValue())
        assert(
          diff <= 600,
          s"attrs TTL[$i] not preserved within tolerance: src=${sTtl} dst=${dTtl}"
        )
      }
    }
  }
}
