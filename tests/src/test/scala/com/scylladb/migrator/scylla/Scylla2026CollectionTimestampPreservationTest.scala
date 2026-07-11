package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row
import com.scylladb.migrator.{ Integration, Scylla2026Compat }
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import org.junit.experimental.categories.Category

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

/** End-to-end coverage for per-element TTL/WRITETIME preservation of non-frozen collections with
  * ScyllaDB as BOTH source and target (`preserveCollectionTimestamps`).
  *
  * ScyllaDB (verified on 2026.2) rejects the Cassandra 5.0 collection-wide `WRITETIME(col)` list
  * form on non-frozen collections but supports the per-element subscript form
  * `WRITETIME(col[key])`. The migrator auto-detects this and reads element metadata via per-row
  * point reads. This test both migrates through that path AND verifies the result using the same
  * subscript form (Scylla can read it back, unlike the Cassandra 5.0-only array form).
  *
  * Runs against a dedicated `scylla2026` service (host port 9048) rather than the shared `scylla`
  * service, because the feature needs ScyllaDB >= 2026.2 and 2026.2 rejects `SimpleStrategy`
  * keyspaces.
  */
@Category(Array(classOf[Integration], classOf[Scylla2026Compat]))
class Scylla2026CollectionTimestampPreservationTest extends munit.FunSuite {

  private val keyspace = "test"
  private val sourceTbl = "collts_src"
  private val targetTbl = "collts_dst"
  private val configFile = "scylla2026-to-scylla2026-collection-timestamps.yaml"

  private val scylla2026: Fixture[CqlSession] = new Fixture[CqlSession]("scylla2026") {
    private var session: CqlSession = null
    def apply(): CqlSession = session
    override def beforeAll(): Unit = {
      session = CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress("localhost", 9048))
        .withLocalDatacenter("datacenter1")
        .withAuthCredentials("dummy", "dummy")
        .build()
      // 2026.2 rejects SimpleStrategy; NetworkTopologyStrategy is required.
      session.execute(
        s"CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH replication = " +
          "{'class':'NetworkTopologyStrategy','replication_factor':1}"
      )
    }
    override def afterAll(): Unit = if (session != null) session.close()
  }

  override def munitFixtures: Seq[Fixture[_]] = Seq(scylla2026)

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

  /** Read per-element metadata via ScyllaDB's subscript form. Arrays are ordered by ascending set
    * element / map key so source and target compare element-wise.
    */
  private def readMeta(session: CqlSession, table: String, id: String): CollectionMeta = {
    val base: Row = session
      .execute(
        s"SELECT name, WRITETIME(name) AS name_wt, tags, attrs " +
          s"FROM ${keyspace}.${table} WHERE id = '${id}'"
      )
      .one()
    assert(base != null, s"expected a row for id=${id} in ${table}")

    val tags = base.getSet("tags", classOf[Integer]).asScala.toList.map(_.intValue()).sorted
    val tagsWt = tags.map { t =>
      session
        .execute(s"SELECT WRITETIME(tags[${t}]) AS wt FROM ${keyspace}.${table} WHERE id = '${id}'")
        .one()
        .getLong("wt")
    }

    val attrs = base
      .getMap("attrs", classOf[String], classOf[Integer])
      .asScala
      .toMap
      .map { case (k, v) => k -> v.intValue() }
    val attrKeys = attrs.keys.toList.sorted
    val (attrsWt, attrsTtl) = attrKeys.map { k =>
      val r = session
        .execute(
          s"SELECT WRITETIME(attrs['${k}']) AS wt, TTL(attrs['${k}']) AS ttl " +
            s"FROM ${keyspace}.${table} WHERE id = '${id}'"
        )
        .one()
      val ttl: Integer = if (r.isNull("ttl")) null else Integer.valueOf(r.getInt("ttl"))
      (r.getLong("wt"), ttl)
    }.unzip

    CollectionMeta(
      tags     = tags,
      tagsWt   = tagsWt,
      attrs    = attrs,
      attrsWt  = attrsWt,
      attrsTtl = attrsTtl,
      name     = base.getString("name"),
      nameWt   = base.getLong("name_wt")
    )
  }

  test(
    "preserves per-element TTL/WRITETIME of non-frozen collections (Scylla source via subscript)"
  ) {
    val session = scylla2026()
    createTable(session, sourceTbl)
    createTable(session, targetTbl)

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

    successfullyPerformMigration(configFile)

    val src = readMeta(session, sourceTbl, id)
    val dst = readMeta(session, targetTbl, id)

    assertEquals(dst.name, src.name)
    assertEquals(dst.nameWt, src.nameWt, "scalar WRITETIME must be preserved")

    assertEquals(dst.tags, src.tags, "set elements must match")
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
