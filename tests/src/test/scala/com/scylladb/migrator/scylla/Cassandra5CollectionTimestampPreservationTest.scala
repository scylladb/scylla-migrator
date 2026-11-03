package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row
import com.scylladb.migrator.{ CassandraCompat, Integration }
import com.scylladb.migrator.SparkUtils.{
  successfullyPerformMigration,
  successfullyPerformMigrationWithAutoResume
}
import com.scylladb.migrator.TestFileUtils
import com.scylladb.migrator.config.MigratorConfig
import com.datastax.spark.connector.rdd.partitioner.dht.{ LongToken, Token }
import org.junit.experimental.categories.Category

import java.net.InetSocketAddress
import java.nio.file.{ Files, Path, Paths }
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
        "(id text PRIMARY KEY, name text, tags set<int>, attrs map<text,int>, " +
        "ftags frozen<set<text>>, fattrs frozen<map<text,int>>)"
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

  // (value, WRITETIME, TTL) of a frozen collection column: a single cell, so scalar metadata.
  private def readFrozen(
    session: CqlSession,
    table: String,
    id: String,
    column: String
  ): (AnyRef, Long, Integer) = {
    val row = session
      .execute(
        s"SELECT ${column}, WRITETIME(${column}) AS wt, TTL(${column}) AS t " +
          s"FROM ${keyspace}.${table} WHERE id = '${id}'"
      )
      .one()
    assert(row != null, s"expected a row for id=${id} in ${table}")
    (
      row.getObject(column),
      row.getLong("wt"),
      if (row.isNull("t")) null else Integer.valueOf(row.getInt("t"))
    )
  }

  private def assertFrozenPreserved(session: CqlSession, id: String, column: String): Unit = {
    val (srcValue, srcWt, srcTtl) = readFrozen(session, sourceTbl, id, column)
    val (dstValue, dstWt, dstTtl) = readFrozen(session, targetTbl, id, column)
    assertEquals(dstValue, srcValue, s"$column value must match")
    assertEquals(dstWt, srcWt, s"$column WRITETIME must be preserved")
    if (srcTtl == null) assertEquals(dstTtl, null, s"$column TTL expected null")
    else {
      assert(dstTtl != null, s"$column TTL expected non-null")
      assert(
        math.abs(srcTtl.intValue() - dstTtl.intValue()) <= 600,
        s"$column TTL not preserved within tolerance: src=${srcTtl} dst=${dstTtl}"
      )
    }
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
    // Frozen collections are single cells: their TTL/WRITETIME go through the base write.
    session.execute(
      s"UPDATE ${keyspace}.${sourceTbl} USING TIMESTAMP 7000000000000 AND TTL 4000000 " +
        s"SET ftags = {'x', 'y'} WHERE id='${id}'"
    )
    session.execute(
      s"UPDATE ${keyspace}.${sourceTbl} USING TIMESTAMP 8000000000000 " +
        s"SET fattrs = {'k': 1, 'j': 2} WHERE id='${id}'"
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

    assertFrozenPreserved(session, id, "ftags")
    assertFrozenPreserved(session, id, "fattrs")

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

  test("removes target collection elements older than every source element") {
    val session = cassandra5()
    createTable(session, sourceTbl)
    createTable(session, targetTbl)

    val id = "r2"
    session.execute(
      s"UPDATE ${keyspace}.${sourceTbl} USING TIMESTAMP 2000000000000 " +
        s"SET tags = tags + {1}, attrs = attrs + {'a': 1} WHERE id='${id}'"
    )
    session.execute(
      s"UPDATE ${keyspace}.${sourceTbl} USING TIMESTAMP 3000000000000 " +
        s"SET tags = tags + {2} WHERE id='${id}'"
    )

    // A populated target: stale target-only elements and a stale value for a shared key, all
    // older than the source, plus one target-only element NEWER than every source element.
    session.execute(
      s"UPDATE ${keyspace}.${targetTbl} USING TIMESTAMP 1000000000000 " +
        s"SET tags = tags + {1, 99}, attrs = attrs + {'a': 42, 'zz': 7} WHERE id='${id}'"
    )
    session.execute(
      s"UPDATE ${keyspace}.${targetTbl} USING TIMESTAMP 9000000000000 " +
        s"SET tags = tags + {777} WHERE id='${id}'"
    )

    successfullyPerformMigration(configFile)

    val dst = readMeta(session, targetTbl, id)
    // Older target-only entries are gone and the shared key carries the source value; the newer
    // target-only element wins by last-write-wins, exactly as it would against any source write.
    assertEquals(dst.tags.sorted, List(1, 2, 777))
    assertEquals(dst.tagsWt, List(2000000000000L, 3000000000000L, 9000000000000L))
    assertEquals(dst.attrs, Map("a" -> 1))
    assertEquals(dst.attrsWt, List(2000000000000L))
  }

  private val resumeConfig = "cassandra5-to-cassandra5-collection-timestamps-resume.yaml"
  private val resumeSourceTbl = "collts_res_src"
  private val resumeTargetTbl = "collts_res_dst"
  private val resumeSavepointsDir = Paths.get("docker/spark-master/cql-collection-resume")

  /** Token ranges recorded by the newest savepoint in `dir` (empty when there is none). */
  private def rangesInLatestSavepoint(dir: Path): Set[(Token[_], Token[_])] =
    if (!Files.exists(dir)) Set.empty
    else
      Files
        .list(dir)
        .iterator()
        .asScala
        .filter(p => Files.isRegularFile(p) && p.getFileName.toString.endsWith(".yaml"))
        .maxByOption(p => (Files.getLastModifiedTime(p).toMillis, p.getFileName.toString))
        .map(p => MigratorConfig.loadFrom(p.toString).getSkipTokenRangesOrEmptySet)
        .getOrElse(Set.empty)

  /** Whether Murmur3 `token` lies in the range `(start, end]`, which may wrap around the ring. */
  private def contains(range: (Token[_], Token[_]), token: Long): Boolean = range match {
    case (LongToken(start), LongToken(end)) =>
      if (start < end) token > start && token <= end else token > start || token <= end
    case other => fail(s"unexpected token range $other")
  }

  /** Rows r1..r8 with per-element TTL/WRITETIMEs in two collections. `attrs` is the first
    * collection-append pass and `tags` the LAST (column-name order). Only r1 has `tags`; every
    * other row's last collection is null, so the ranges holding them get no rows in the final pass.
    */
  private def populateTwoCollectionSource(session: CqlSession): Seq[String] = {
    val ids = (1 to 8).map(i => s"r$i")
    ids.zipWithIndex.foreach { case (id, i) =>
      session.execute(
        s"INSERT INTO ${keyspace}.${resumeSourceTbl} (id, name) VALUES ('${id}', 'n$i') " +
          "USING TIMESTAMP 1000000000000"
      )
      session.execute(
        s"UPDATE ${keyspace}.${resumeSourceTbl} USING TIMESTAMP ${2000000000000L + i} " +
          s"AND TTL 2000000 SET attrs = attrs + {'k$i': $i} WHERE id='${id}'"
      )
    }
    Seq(10 -> 3000000000000L, 20 -> 4000000000000L).foreach { case (v, wt) =>
      session.execute(
        s"UPDATE ${keyspace}.${resumeSourceTbl} USING TIMESTAMP ${wt} AND TTL 3000000 " +
          s"SET tags = tags + {${v}} WHERE id='r1'"
      )
    }
    ids
  }

  /** Token ranges recorded by each savepoint in `dir`, oldest first. */
  private def rangesPerSavepoint(dir: Path): Seq[Set[(Token[_], Token[_])]] =
    Files
      .list(dir)
      .iterator()
      .asScala
      .filter(p => Files.isRegularFile(p) && p.getFileName.toString.endsWith(".yaml"))
      .toSeq
      .sortBy(_.getFileName.toString)
      .map(p => MigratorConfig.loadFrom(p.toString).getSkipTokenRangesOrEmptySet)

  test("per-element collections are written in bounded batches, each recorded when complete") {
    val session = cassandra5()
    createTable(session, resumeSourceTbl)
    session.execute(s"DROP TABLE IF EXISTS ${keyspace}.${resumeTargetTbl}")
    createTable(session, resumeTargetTbl)
    TestFileUtils.deleteRecursive(resumeSavepointsDir.toFile)
    val ids = populateTwoCollectionSource(session)

    val cachedBefore =
      org.apache.spark.sql.SparkSession.active.sparkContext.getPersistentRDDs.keySet
    val batchKey = "scylla.migrator.collectionBatchPartitions"
    sys.props(batchKey) = "1" // one source partition per batch
    try successfullyPerformMigration(resumeConfig)
    finally sys.props.remove(batchKey)

    ids.foreach { id =>
      val src = readMeta(session, resumeSourceTbl, id)
      val dst = readMeta(session, resumeTargetTbl, id)
      assertEquals(dst.tags.sorted, src.tags.sorted, id)
      assertEquals(dst.tagsWt, src.tagsWt, id)
      assertEquals(dst.attrs, src.attrs, id)
      assertEquals(dst.attrsWt, src.attrsWt, id)
    }

    // A savepoint after every batch, each adding that batch's ranges to the ones before.
    val perSavepoint = rangesPerSavepoint(resumeSavepointsDir)
    val distinct = perSavepoint.distinct
    assert(distinct.size > 2, s"expected several batches, got savepoints $perSavepoint")
    distinct.sliding(2).foreach {
      case Seq(earlier, later) =>
        assert(earlier.subsetOf(later) && earlier != later, "each batch must add ranges")
      case _ =>
    }
    val tokens = session
      .execute(s"SELECT token(id) AS t FROM ${keyspace}.${resumeSourceTbl}")
      .all()
      .asScala
      .map(_.getLong("t"))
    tokens.foreach(t => assert(distinct.last.exists(contains(_, t)), s"token $t not recorded"))

    // Every batch released its cache.
    assertEquals(
      org.apache.spark.sql.SparkSession.active.sparkContext.getPersistentRDDs.keySet,
      cachedBefore,
      "no source batch may stay cached after the migration"
    )
  }

  test("an interrupted multi-pass CQL migration records no range and resumes to the full result") {
    val session = cassandra5()
    createTable(session, resumeSourceTbl)
    TestFileUtils.deleteRecursive(resumeSavepointsDir.toFile)

    val ids = populateTwoCollectionSource(session)

    // The target lacks `tags`, so the base write and the `attrs` pass succeed and the final
    // `tags` pass fails. One batch holds every row, so they all reach that point; with several
    // batches (the default depends on the machine's cores) the run would stop after the first.
    session.execute(s"DROP TABLE IF EXISTS ${keyspace}.${resumeTargetTbl}")
    session.execute(
      s"CREATE TABLE ${keyspace}.${resumeTargetTbl} " +
        "(id text PRIMARY KEY, name text, attrs map<text,int>, " +
        "ftags frozen<set<text>>, fattrs frozen<map<text,int>>)"
    )
    val batchKey = "scylla.migrator.collectionBatchPartitions"
    sys.props(batchKey) = "0" // a single batch
    try intercept[Throwable](successfullyPerformMigration(resumeConfig))
    finally sys.props.remove(batchKey)

    ids.foreach { id =>
      val partial = session
        .execute(s"SELECT name, attrs FROM ${keyspace}.${resumeTargetTbl} WHERE id = '${id}'")
        .one()
      assert(partial != null, s"the base write should have written ${id}")
      assert(!partial.getMap("attrs", classOf[String], classOf[Integer]).isEmpty)
    }
    assertEquals(
      rangesInLatestSavepoint(resumeSavepointsDir),
      Set.empty[(Token[_], Token[_])],
      "no token range may be recorded before the final collection-append pass has written it"
    )

    // Fix the target and resume from the savepoint: every range is replayed.
    session.execute(s"ALTER TABLE ${keyspace}.${resumeTargetTbl} ADD tags set<int>")
    successfullyPerformMigrationWithAutoResume(resumeConfig)

    ids.foreach { id =>
      val src = readMeta(session, resumeSourceTbl, id)
      val dst = readMeta(session, resumeTargetTbl, id)
      assertEquals(dst.name, src.name, id)
      assertEquals(dst.nameWt, src.nameWt, id)
      assertEquals(dst.tags.sorted, src.tags.sorted, id)
      assertEquals(dst.tagsWt, src.tagsWt, id)
      assertEquals(dst.attrs, src.attrs, id)
      assertEquals(dst.attrsWt, src.attrsWt, id)
      dst.attrsTtl.zip(src.attrsTtl).foreach { case (dTtl, sTtl) =>
        assert(dTtl != null && math.abs(sTtl - dTtl) <= 600, s"$id TTL src=$sTtl dst=$dTtl")
      }
    }
    val tagsTtl = session
      .execute(s"SELECT TTL(tags) AS t FROM ${keyspace}.${resumeTargetTbl} WHERE id = 'r1'")
      .one()
      .getList("t", classOf[Integer])
      .asScala
    assert(tagsTtl.forall(t => t != null && math.abs(3000000 - t) <= 600), s"tags TTLs $tagsTtl")

    // The successful final pass records every input range, including ranges in which it wrote
    // no rows because their rows' last collection is null.
    val recorded = rangesInLatestSavepoint(resumeSavepointsDir)
    val tokens = session
      .execute(s"SELECT id, token(id) AS t FROM ${keyspace}.${resumeSourceTbl}")
      .all()
      .asScala
      .map(r => r.getString("id") -> r.getLong("t"))
      .toMap
    tokens.foreach { case (id, token) =>
      assert(recorded.exists(contains(_, token)), s"the range holding ${id} was not recorded")
    }
    assert(
      recorded.exists(range =>
        !contains(range, tokens("r1")) && tokens.values.exists(contains(range, _))
      ),
      "expected a recorded range whose rows all have a null last collection"
    )
  }
}
