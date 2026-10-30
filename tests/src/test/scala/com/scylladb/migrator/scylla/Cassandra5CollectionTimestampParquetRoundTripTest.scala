package com.scylladb.migrator.scylla

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row
import com.scylladb.migrator.{ CassandraCompat, Integration, TestFileUtils }
import com.scylladb.migrator.SparkUtils.{
  successfullyPerformMigration,
  successfullyPerformMigrationWithAutoResume
}
import com.scylladb.migrator.config.MigratorConfig
import org.junit.experimental.categories.Category

import java.net.InetSocketAddress
import java.nio.file.{ Files, Path, Paths }
import scala.jdk.CollectionConverters._

/** End-to-end coverage for per-element TTL/WRITETIME preservation of non-frozen collections across
  * the Parquet intermediate format: Cassandra 5.0 -> Parquet -> Cassandra 5.0.
  *
  * The export writes each non-frozen collection's per-element metadata as `__migrator_meta_*` array
  * sidecars; the restore re-hydrates them into collection-append passes. Cassandra 5.0 is used for
  * both ends because ScyllaDB cannot read back `WRITETIME()`/`TTL()` of non-frozen collections for
  * verification.
  */
@Category(Array(classOf[Integration], classOf[CassandraCompat]))
class Cassandra5CollectionTimestampParquetRoundTripTest extends munit.FunSuite {

  private val keyspace = "test"
  private val sourceTbl = "collts_pq_src"
  private val targetTbl = "collts_pq_dst"
  private val exportConfig = "cassandra5-to-parquet-collection-timestamps.yaml"
  private val restoreConfig = "parquet-to-cassandra5-collection-timestamps.yaml"
  private val parquetDir = Paths.get("docker/parquet/collection-timestamps")
  private val resumeConfig = "parquet-to-cassandra5-collection-timestamps-resume.yaml"
  private val resumeTargetTbl = "collts_pq_resume_dst"
  private val resumeSavepointsDir = Paths.get("docker/spark-master/collection-timestamps-resume")

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
    // Frozen collections are single cells: their TTL/WRITETIME go through the base write.
    session.execute(
      s"UPDATE ${keyspace}.${sourceTbl} USING TIMESTAMP 7000000000000 AND TTL 4000000 " +
        s"SET ftags = {'x', 'y'} WHERE id='${id}'"
    )
    session.execute(
      s"UPDATE ${keyspace}.${sourceTbl} USING TIMESTAMP 8000000000000 " +
        s"SET fattrs = {'k': 1, 'j': 2} WHERE id='${id}'"
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

    assertFrozenPreserved(session, id, "ftags")
    assertFrozenPreserved(session, id, "fattrs")

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

  /** Files listed as processed by the newest savepoint in `dir` (empty when there is none). */
  private def skippedFilesInLatestSavepoint(dir: Path): Set[String] =
    if (!Files.exists(dir)) Set.empty
    else
      Files
        .list(dir)
        .iterator()
        .asScala
        .filter(p => Files.isRegularFile(p) && p.getFileName.toString.endsWith(".yaml"))
        .maxByOption(p => (Files.getLastModifiedTime(p).toMillis, p.getFileName.toString))
        .flatMap(p => MigratorConfig.loadFrom(p.toString).skipParquetFiles)
        .getOrElse(Set.empty)

  test("an interrupted multi-pass restore records no file and resumes to the full result") {
    val session = cassandra5()
    createTable(session, sourceTbl)
    TestFileUtils.deleteRecursive(parquetDir.toFile)
    TestFileUtils.deleteRecursive(resumeSavepointsDir.toFile)

    val id = "r1"
    session.execute(
      s"INSERT INTO ${keyspace}.${sourceTbl} (id, name) VALUES ('${id}', 'alice') " +
        "USING TIMESTAMP 1000000000000"
    )
    Seq(20 -> 2000000000000L, 10 -> 1000000000000L).foreach { case (v, wt) =>
      session.execute(
        s"UPDATE ${keyspace}.${sourceTbl} USING TIMESTAMP ${wt} AND TTL 2000000 " +
          s"SET tags = tags + {${v}} WHERE id='${id}'"
      )
    }
    Seq(("b", 2, 2000000000000L), ("a", 1, 1000000000000L)).foreach { case (k, v, wt) =>
      session.execute(
        s"UPDATE ${keyspace}.${sourceTbl} USING TIMESTAMP ${wt} " +
          s"SET attrs = attrs + {'${k}': ${v}} WHERE id='${id}'"
      )
    }
    successfullyPerformMigration(exportConfig)

    // The target lacks `tags`, the LAST collection-append pass (passes run in column-name order),
    // so the base write and the `attrs` pass succeed and the `tags` pass fails.
    session.execute(s"DROP TABLE IF EXISTS ${keyspace}.${resumeTargetTbl}")
    session.execute(
      s"CREATE TABLE ${keyspace}.${resumeTargetTbl} " +
        "(id text PRIMARY KEY, name text, attrs map<text,int>, " +
        "ftags frozen<set<text>>, fattrs frozen<map<text,int>>)"
    )
    intercept[Throwable](successfullyPerformMigration(resumeConfig))

    val partial = session
      .execute(s"SELECT name, attrs FROM ${keyspace}.${resumeTargetTbl} WHERE id = '${id}'")
      .one()
    assert(partial != null, "the base write should have run before the failing pass")
    assertEquals(partial.getString("name"), "alice")
    assertEquals(
      skippedFilesInLatestSavepoint(resumeSavepointsDir),
      Set.empty[String],
      "no Parquet file may be recorded as processed before every write pass has succeeded"
    )

    // Fix the target and resume from the savepoint: every file is replayed.
    session.execute(s"ALTER TABLE ${keyspace}.${resumeTargetTbl} ADD tags set<int>")
    successfullyPerformMigrationWithAutoResume(resumeConfig)

    val src = readMeta(session, sourceTbl, id)
    val dst = readMeta(session, resumeTargetTbl, id)
    assertEquals(dst.name, src.name)
    assertEquals(dst.nameWt, src.nameWt)
    assertEquals(dst.tags.sorted, src.tags.sorted, "the interrupted pass must be completed")
    assertEquals(dst.tagsWt, src.tagsWt)
    assertEquals(dst.attrs, src.attrs)
    assertEquals(dst.attrsWt, src.attrsWt)
    assert(
      skippedFilesInLatestSavepoint(resumeSavepointsDir).nonEmpty,
      "the successful resume should record the processed files"
    )
  }
}
