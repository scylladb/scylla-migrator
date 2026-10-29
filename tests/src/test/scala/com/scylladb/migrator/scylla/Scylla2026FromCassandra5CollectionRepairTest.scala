package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.scylladb.migrator.{ Integration, Scylla2026Compat }
import com.scylladb.migrator.SparkUtils.performValidation
import org.junit.experimental.categories.Category

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

/** Validation and `copyMissingRows` repair from a Cassandra 5.0 source to a ScyllaDB 2026.2 target.
  * The target only exposes per-element collection metadata via the subscript form, so non-frozen
  * collections are compared by value, but the source supplies the element metadata the repair
  * needs. Requires both the `cassandra5` and `scylla2026` services.
  */
@Category(Array(classOf[Integration], classOf[Scylla2026Compat]))
class Scylla2026FromCassandra5CollectionRepairTest extends munit.FunSuite {

  private val keyspace = "test"
  private val sourceTbl = "collts_xrep_src"
  private val targetTbl = "collts_xrep_dst"
  private val configFile = "cassandra5-to-scylla2026-collection-timestamps-repair.yaml"

  private def session(port: Int, replication: String): Fixture[CqlSession] =
    new Fixture[CqlSession](s"cql-$port") {
      private var s: CqlSession = null
      def apply(): CqlSession = s
      override def beforeAll(): Unit = {
        s = CqlSession
          .builder()
          .addContactPoint(new InetSocketAddress("localhost", port))
          .withLocalDatacenter("datacenter1")
          .withAuthCredentials("dummy", "dummy")
          .build()
        s.execute(
          s"CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH replication = " +
            s"{'class':'${replication}','replication_factor':1}"
        )
      }
      override def afterAll(): Unit = if (s != null) s.close()
    }

  private val cassandra5 = session(9047, "SimpleStrategy")
  // 2026.2 rejects SimpleStrategy; NetworkTopologyStrategy is required.
  private val scylla2026 = session(9048, "NetworkTopologyStrategy")

  override def munitFixtures: Seq[Fixture[_]] = Seq(cassandra5, scylla2026)

  private def createTable(session: CqlSession, table: String): Unit = {
    session.execute(s"DROP TABLE IF EXISTS ${keyspace}.${table}")
    session.execute(
      s"CREATE TABLE ${keyspace}.${table} " +
        "(id text PRIMARY KEY, name text, tags set<int>, attrs map<text,int>)"
    )
  }

  /** (elements, WRITETIMEs, TTLs) per collection, ordered by set element / map key. */
  private case class Meta(
    tags: List[Int],
    tagsWt: List[Long],
    attrs: Map[String, Int],
    attrsWt: List[Long],
    attrsTtl: List[Integer]
  )

  private def ttlOf(r: com.datastax.oss.driver.api.core.cql.Row, col: String): Integer =
    if (r.isNull(col)) null else Integer.valueOf(r.getInt(col))

  // Cassandra 5.0: collection-wide form, already in sorted element order.
  private def readSource(id: String): Meta = {
    val r = cassandra5()
      .execute(
        s"SELECT tags, WRITETIME(tags) AS tags_wt, attrs, WRITETIME(attrs) AS attrs_wt, " +
          s"TTL(attrs) AS attrs_ttl FROM ${keyspace}.${sourceTbl} WHERE id = '${id}'"
      )
      .one()
    Meta(
      r.getSet("tags", classOf[Integer]).asScala.toList.map(_.intValue()).sorted,
      r.getList("tags_wt", classOf[java.lang.Long]).asScala.toList.map(_.longValue()),
      r.getMap("attrs", classOf[String], classOf[Integer])
        .asScala
        .toMap
        .map { case (k, v) => k -> v.intValue() },
      r.getList("attrs_wt", classOf[java.lang.Long]).asScala.toList.map(_.longValue()),
      r.getList("attrs_ttl", classOf[Integer]).asScala.toList
    )
  }

  // ScyllaDB 2026.2: per-element subscript form.
  private def readTarget(id: String): Meta = {
    val s = scylla2026()
    val from = s"FROM ${keyspace}.${targetTbl} WHERE id = '${id}'"
    val base = s.execute(s"SELECT tags, attrs $from").one()
    assert(base != null, s"expected a row for id=${id} in ${targetTbl}")
    val tags = base.getSet("tags", classOf[Integer]).asScala.toList.map(_.intValue()).sorted
    val attrs = base
      .getMap("attrs", classOf[String], classOf[Integer])
      .asScala
      .toMap
      .map { case (k, v) => k -> v.intValue() }
    val tagsWt =
      tags.map(t => s.execute(s"SELECT WRITETIME(tags[$t]) AS wt $from").one().getLong("wt"))
    val (attrsWt, attrsTtl) = attrs.keys.toList.sorted.map { k =>
      val r =
        s.execute(s"SELECT WRITETIME(attrs['$k']) AS wt, TTL(attrs['$k']) AS t $from").one()
      (r.getLong("wt"), ttlOf(r, "t"))
    }.unzip
    Meta(tags, tagsWt, attrs, attrsWt, attrsTtl)
  }

  test("copyMissingRows restores element TTL/WRITETIME on a subscript-only target") {
    createTable(cassandra5(), sourceTbl)
    createTable(scylla2026(), targetTbl) // target starts empty

    val id = "r1"
    val src = cassandra5()
    src.execute(
      s"INSERT INTO ${keyspace}.${sourceTbl} (id, name) VALUES ('${id}', 'alice') " +
        "USING TIMESTAMP 1000000000000"
    )
    Seq(30 -> 3000000000000L, 10 -> 1000000000000L, 20 -> 2000000000000L).foreach { case (v, wt) =>
      src.execute(
        s"UPDATE ${keyspace}.${sourceTbl} USING TIMESTAMP ${wt} SET tags = tags + {${v}} " +
          s"WHERE id='${id}'"
      )
    }
    Seq(("b", 2, 2000000000000L, Some(1000000)), ("a", 1, 1000000000000L, None)).foreach {
      case (k, v, wt, ttlOpt) =>
        val using =
          ttlOpt.fold(s"USING TIMESTAMP ${wt}")(t => s"USING TIMESTAMP ${wt} AND TTL ${t}")
        src.execute(
          s"UPDATE ${keyspace}.${sourceTbl} ${using} SET attrs = attrs + {'${k}': ${v}} " +
            s"WHERE id='${id}'"
        )
    }

    // The missing row is detected (snapshot before the repair) and repaired.
    assertEquals(performValidation(configFile), 1, "Should detect the missing target row")

    val expected = readSource(id)
    val actual = readTarget(id)
    assertEquals(actual.tags, expected.tags)
    assertEquals(actual.tagsWt, expected.tagsWt, "set element WRITETIMEs must be preserved")
    assertEquals(actual.attrs, expected.attrs)
    assertEquals(actual.attrsWt, expected.attrsWt, "map element WRITETIMEs must be preserved")
    actual.attrsTtl.zip(expected.attrsTtl).foreach { case (dTtl, sTtl) =>
      if (sTtl == null) assertEquals(dTtl, null)
      else assert(dTtl != null && math.abs(sTtl - dTtl) <= 600, s"TTL src=$sTtl dst=$dTtl")
    }

    // Collections are compared by value against this target; scalars still with metadata.
    assertEquals(performValidation(configFile), 0, "Re-validation after repair should pass")
  }
}
