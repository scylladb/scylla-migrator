package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row
import com.scylladb.migrator.{ CassandraCompat, Integration }
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import org.junit.experimental.categories.Category

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

/** End-to-end coverage for per-element TTL/WRITETIME preservation of non-frozen collections
  * (`preserveCollectionTimestamps`).
  *
  * The migration runs Cassandra 5.0 -> Cassandra 5.0 (the target host in the config is
  * `cassandra5`, remapped to the C*5.0 host port by the harness) because ScyllaDB cannot read back
  * `WRITETIME()`/`TTL()` of non-frozen collections for verification, while Cassandra 5.0 can. The
  * write path is plain CQL, identical whether the target is Scylla or Cassandra.
  */
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra5CollectionTimestampPreservationTest extends munit.FunSuite {

  private val keyspace = "test"
  private val sourceTbl = "collts_src"
  private val targetTbl = "collts_dst"
  private val configFile = "cassandra5-to-cassandra5-collection-timestamps.yaml"

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

  test("preserves per-element TTL/WRITETIME of non-frozen set and map columns") {
    val session = cassandra5()
    createTable(session, sourceTbl)
    createTable(session, targetTbl)

    val id = "r1"
    // Scalar written with an explicit timestamp (exercises the base write path alongside appends).
    session.execute(
      s"INSERT INTO ${keyspace}.${sourceTbl} (id, name) VALUES ('${id}', 'alice') " +
        s"USING TIMESTAMP 1000000000000"
    )

    // Set elements, each with its own WRITETIME, no TTL. 5+ elements to exceed small-collection
    // special cases, inserted out of natural order.
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

    // Map entries with distinct WRITETIMEs, mixed TTLs, keys inserted out of sort order (exercises
    // the connector's unordered decode + key-sort re-alignment). 6 entries => HashMap on decode.
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

    successfullyPerformMigration(configFile)

    val src = readMeta(session, sourceTbl, id)
    val dst = readMeta(session, targetTbl, id)

    // Scalar preserved.
    assertEquals(dst.name, src.name)
    assertEquals(dst.nameWt, src.nameWt, "scalar WRITETIME must be preserved")

    // Set: same elements and element-aligned WRITETIMEs (Cassandra returns both in sorted order).
    assertEquals(dst.tags.sorted, src.tags.sorted, "set elements must match")
    assertEquals(dst.tagsWt, src.tagsWt, "set element WRITETIMEs must be preserved")

    // Map: same entries and element-aligned WRITETIMEs (both returned in key-sorted order).
    assertEquals(dst.attrs, src.attrs, "map entries must match")
    assertEquals(dst.attrsWt, src.attrsWt, "map element WRITETIMEs must be preserved")

    // Map TTLs: null stays null; non-null preserved within a small tolerance for elapsed time.
    assertEquals(dst.attrsTtl.length, src.attrsTtl.length)
    dst.attrsTtl.zip(src.attrsTtl).zipWithIndex.foreach { case ((dTtl, sTtl), i) =>
      if (sTtl == null) assert(dTtl == null, s"attrs TTL[$i] expected null, got ${dTtl}")
      else {
        assert(dTtl != null, s"attrs TTL[$i] expected non-null")
        // The target TTL may differ from the source by the read->write elapsed time (and the
        // source keeps aging), so compare with an absolute tolerance rather than a direction.
        val diff = math.abs(sTtl.intValue() - dTtl.intValue())
        assert(
          diff <= 600,
          s"attrs TTL[$i] not preserved within tolerance: src=${sTtl} dst=${dTtl}"
        )
      }
    }
  }
}
