package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.scylladb.migrator.{ Integration, Scylla2026Compat }
import com.scylladb.migrator.SparkUtils.performValidation
import org.junit.experimental.categories.Category

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

/** Validation and `copyMissingRows` repair between Cassandra 5.0 and ScyllaDB 2026.2, in both
  * directions. ScyllaDB only exposes per-element collection metadata via the subscript form, which
  * the validator reads with per-element point lookups; Cassandra 5.0 returns it in the scan.
  * Requires both the `cassandra5` and `scylla2026` services.
  */
@Category(Array(classOf[Integration], classOf[Scylla2026Compat]))
class Scylla2026FromCassandra5CollectionRepairTest extends munit.FunSuite {

  private val keyspace = "test"
  private val sourceTbl = "collts_xrep_src"
  private val targetTbl = "collts_xrep_dst"
  private val configFile = "cassandra5-to-scylla2026-collection-timestamps-repair.yaml"
  private val reverseSourceTbl = "collts_xrev_src"
  private val reverseTargetTbl = "collts_xrev_dst"
  private val reverseConfigFile = "scylla2026-to-cassandra5-collection-timestamps-repair.yaml"

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
  private def readCassandra5(table: String, id: String): Meta = {
    val r = cassandra5()
      .execute(
        s"SELECT tags, WRITETIME(tags) AS tags_wt, attrs, WRITETIME(attrs) AS attrs_wt, " +
          s"TTL(attrs) AS attrs_ttl FROM ${keyspace}.${table} WHERE id = '${id}'"
      )
      .one()
    assert(r != null, s"expected a row for id=${id} in ${table}")
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
  private def readScylla(table: String, id: String): Meta = {
    val s = scylla2026()
    val from = s"FROM ${keyspace}.${table} WHERE id = '${id}'"
    val base = s.execute(s"SELECT tags, attrs $from").one()
    assert(base != null, s"expected a row for id=${id} in ${table}")
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

  private def populate(session: CqlSession, table: String, id: String): Unit = {
    session.execute(
      s"INSERT INTO ${keyspace}.${table} (id, name) VALUES ('${id}', 'alice') " +
        "USING TIMESTAMP 1000000000000"
    )
    Seq(30 -> 3000000000000L, 10 -> 1000000000000L, 20 -> 2000000000000L).foreach { case (v, wt) =>
      session.execute(
        s"UPDATE ${keyspace}.${table} USING TIMESTAMP ${wt} SET tags = tags + {${v}} " +
          s"WHERE id='${id}'"
      )
    }
    Seq(("b", 2, 2000000000000L, Some(1000000)), ("a", 1, 1000000000000L, None)).foreach {
      case (k, v, wt, ttlOpt) =>
        val using =
          ttlOpt.fold(s"USING TIMESTAMP ${wt}")(t => s"USING TIMESTAMP ${wt} AND TTL ${t}")
        session.execute(
          s"UPDATE ${keyspace}.${table} ${using} SET attrs = attrs + {'${k}': ${v}} " +
            s"WHERE id='${id}'"
        )
    }
  }

  /** Same values, but one set element gets a newer WRITETIME on the target. */
  private def tamper(session: CqlSession, table: String, id: String): Unit =
    session.execute(
      s"UPDATE ${keyspace}.${table} USING TIMESTAMP 9000000000000 SET tags = tags + {10} " +
        s"WHERE id='${id}'"
    )

  private def assertRepaired(expected: Meta, actual: Meta): Unit = {
    assertEquals(actual.tags, expected.tags)
    assertEquals(actual.tagsWt, expected.tagsWt, "set element WRITETIMEs must be preserved")
    assertEquals(actual.attrs, expected.attrs)
    assertEquals(actual.attrsWt, expected.attrsWt, "map element WRITETIMEs must be preserved")
    actual.attrsTtl.zip(expected.attrsTtl).foreach { case (dTtl, sTtl) =>
      if (sTtl == null) assertEquals(dTtl, null)
      else assert(dTtl != null && math.abs(sTtl - dTtl) <= 600, s"TTL src=$sTtl dst=$dTtl")
    }
  }

  test("Cassandra 5.0 -> ScyllaDB 2026.2: repair and validation of element TTL/WRITETIME") {
    createTable(cassandra5(), sourceTbl)
    createTable(scylla2026(), targetTbl) // target starts empty
    val id = "r1"
    populate(cassandra5(), sourceTbl, id)

    // The missing row is detected (snapshot before the repair) and repaired.
    assertEquals(performValidation(configFile), 1, "Should detect the missing target row")
    assertRepaired(readCassandra5(sourceTbl, id), readScylla(targetTbl, id))

    // Per-element metadata is compared against the subscript-only target.
    assertEquals(performValidation(configFile), 0, "Re-validation after repair should pass")
    tamper(scylla2026(), targetTbl, id)
    assertEquals(performValidation(configFile), 1, "a differing element WRITETIME must fail")
  }

  test("ScyllaDB 2026.2 -> Cassandra 5.0: repair and validation of element TTL/WRITETIME") {
    createTable(scylla2026(), reverseSourceTbl)
    createTable(cassandra5(), reverseTargetTbl) // target starts empty
    val id = "r2"
    populate(scylla2026(), reverseSourceTbl, id)

    assertEquals(performValidation(reverseConfigFile), 1, "Should detect the missing target row")
    assertRepaired(readScylla(reverseSourceTbl, id), readCassandra5(reverseTargetTbl, id))

    assertEquals(performValidation(reverseConfigFile), 0, "Re-validation after repair should pass")
    tamper(cassandra5(), reverseTargetTbl, id)
    assertEquals(
      performValidation(reverseConfigFile),
      1,
      "a differing element WRITETIME must fail"
    )
  }
}
