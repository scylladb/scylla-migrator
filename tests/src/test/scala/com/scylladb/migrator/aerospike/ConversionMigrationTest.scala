package com.scylladb.migrator.aerospike

import com.aerospike.client.{ Bin, Key }
import com.aerospike.client.policy.WritePolicy
import com.datastax.spark.connector.cql.CassandraConnector
import com.scylladb.migrator.{ Migrator, SparkUtils }
import com.scylladb.migrator.config.MigratorConfig
import io.circe.yaml.parser

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

/** Conversion assertions against both databases, including the destination connector's casts. */
class ConversionMigrationTest extends MigratorSuite {
  private val setName = "conversiontest"

  private def createTable(columns: String): Unit = {
    targetScylla().execute(s"CREATE TABLE $keyspace.$setName ($columns)")
    // Each test recreates this table with different types. Cached connector sessions can retain
    // prepared statements from the previous schema, including its map-key converters.
    CassandraConnector.evictCache()
  }

  private def put(key: String, bins: Bin*): Unit = {
    val policy = new WritePolicy()
    policy.sendKey = true
    retryPut(policy, new Key(aerospikeNamespace, setName, key), bins: _*)
  }

  private def migrate(
    schema: String,
    sourceOptions: String = "",
    renames: String = "",
    keyMode: String = "string"
  ): Unit = {
    val host = sys.env.getOrElse("AEROSPIKE_HOST", "localhost")
    val port = sys.env.getOrElse("AEROSPIKE_PORT", "3000")
    val targetHost = sys.env.getOrElse("SCYLLA_HOST", "localhost")
    val targetPort = sys.env.getOrElse("SCYLLA_PORT", "9042")
    val dc = sys.env.getOrElse("SCYLLA_DATACENTER", "datacenter1")
    val yaml = s"""source:
                  |  type: aerospike
                  |  hosts: [$host]
                  |  port: $port
                  |  namespace: $aerospikeNamespace
                  |  set: $setName
                  |  keyMode: $keyMode
                  |  schema: $schema
                  |  splitCount: 1
                  |${sourceOptions.linesIterator.map("  " + _).mkString("\n")}
                  |target:
                  |  type: scylla
                  |  host: $targetHost
                  |  port: $targetPort
                  |  localDC: $dc
                  |  keyspace: $keyspace
                  |  table: $setName
                  |  consistencyLevel: LOCAL_QUORUM
                  |  connections: 1
                  |  stripTrailingZerosForDecimals: false
                  |$renames
                  |""".stripMargin
    val config = parser.parse(yaml).flatMap(_.as[MigratorConfig]).fold(throw _, identity)
    SparkUtils.withSparkSession()(spark => Migrator.migrate(config)(spark))
  }

  private def rows = targetScylla().execute(s"SELECT * FROM $keyspace.$setName").all().asScala.toSeq

  private def assertRejected(fragment: String)(run: => Unit): Unit = {
    val error = intercept[Exception](run)
    val messages = Iterator
      .iterate[Throwable](error)(_.getCause)
      .takeWhile(_ != null)
      .map(e => Option(e.getMessage).getOrElse(""))
      .mkString("\n")
    assert(messages.toLowerCase.contains(fragment.toLowerCase), messages)
    assertEquals(rows.size, 0)
  }

  withSet(setName).test("mixed numeric bins and collection members preserve large integers") { _ =>
    createTable("aero_key text PRIMARY KEY, scalar text, items list<text>, attrs map<text,text>")
    val large = 9007199254740993L
    put(
      "integer",
      new Bin("scalar", large),
      new Bin("items", List[Any](large).asJava),
      new Bin("attrs", Map[String, Any]("number" -> large).asJava)
    )
    put(
      "double",
      new Bin("scalar", 1.5),
      new Bin("items", List[Any](1.5).asJava),
      new Bin("attrs", Map[String, Any]("number" -> 1.5).asJava)
    )
    migrate("{scalar: string, items: 'list<string>', attrs: 'map<string,string>'}")
    val actual = rows.map(r => r.getString("aero_key") -> r).toMap
    assertEquals(actual("integer").getString("scalar"), large.toString)
    assertEquals(
      actual("integer").getList("items", classOf[String]).asScala.toSeq,
      Seq(large.toString)
    )
    assertEquals(
      actual("integer").getMap("attrs", classOf[String], classOf[String]).get("number"),
      large.toString
    )
    assertEquals(actual("double").getString("scalar"), "1.5")
  }

  withSet(setName).test("safe numeric target casts and nested collections preserve values") { _ =>
    createTable(
      "aero_key text PRIMARY KEY, count int, fraction float, whole bigint, nested list<frozen<map<text,int>>>"
    )
    put(
      "key",
      new Bin("count", 42L),
      new Bin("fraction", 1.5),
      new Bin("whole", 12.0),
      new Bin("nested", List(Map[String, Any]("answer" -> 42L).asJava).asJava)
    )
    migrate("{count: long, fraction: double, whole: double, nested: 'list<map<string,long>>'}")
    val row = rows.head
    assertEquals(row.getInt("count"), 42)
    assertEquals(row.getFloat("fraction"), 1.5f)
    assertEquals(row.getLong("whole"), 12L)
    val nested = row.getList("nested", classOf[java.util.Map[String, Integer]])
    assertEquals(nested.get(0).get("answer"), Integer.valueOf(42))
  }

  withSet(setName).test(
    "source metadata names and internal names survive custom and chained renames"
  ) { _ =>
    createTable(
      "id text PRIMARY KEY, aero_key text, aero_ttl bigint, aero_generation bigint, query_text text"
    )
    put(
      "identity",
      new Bin("aero_key", "user value"),
      new Bin("aero_ttl", 17L),
      new Bin("aero_generation", 23L),
      new Bin("solr_query", "ordinary data")
    )
    migrate(
      "{aero_key: string, aero_ttl: long, aero_generation: long, solr_query: string}",
      "keyColumn: record_key",
      """renames:
        |  - {from: record_key, to: temporary}
        |  - {from: temporary, to: id}
        |  - {from: solr_query, to: query_text}""".stripMargin
    )
    val row = rows.head
    assertEquals(row.getString("id"), "identity")
    assertEquals(row.getString("aero_key"), "user value")
    assertEquals(row.getLong("aero_ttl"), 17L)
    assertEquals(row.getLong("aero_generation"), 23L)
    assertEquals(row.getString("query_text"), "ordinary data")
  }

  withSet(setName).test("custom metadata names preserve user bins alongside TTL and generation") {
    _ =>
      createTable(
        "aero_key text PRIMARY KEY, aero_ttl text, aero_generation text, remaining int, revision int"
      )
      put("key", new Bin("aero_ttl", "user ttl"), new Bin("aero_generation", "user generation"))
      migrate(
        "{aero_ttl: string, aero_generation: string}",
        """preserveTTL: true
          |preserveGeneration: true
          |ttlColumn: remaining
          |generationColumn: revision""".stripMargin
      )
      val row = rows.head
      assertEquals(row.getString("aero_ttl"), "user ttl")
      assertEquals(row.getString("aero_generation"), "user generation")
      assertEquals(row.getInt("remaining"), -1)
      assert(row.getInt("revision") > 0)
  }

  withSet(setName).test("binary record identity and frozen empty collections survive migration") {
    _ =>
      createTable(
        "aero_key blob PRIMARY KEY, items frozen<list<bigint>>, attrs frozen<map<text,bigint>>"
      )
      val policy = new WritePolicy()
      policy.sendKey = true
      val key = Array[Byte](0, -1, 17)
      retryPut(
        policy,
        new Key(aerospikeNamespace, setName, key),
        new Bin("items", List.empty[Any].asJava),
        new Bin("attrs", Map.empty[String, Any].asJava)
      )
      migrate(
        "{items: 'list<long>', attrs: 'map<string,long>'}",
        keyMode = "binary"
      )
      val row = rows.head
      assertEquals(row.getByteBuffer("aero_key"), ByteBuffer.wrap(key))
      assert(!row.isNull("items"))
      assert(!row.isNull("attrs"))
      assert(row.getList("items", classOf[java.lang.Long]).isEmpty)
      assert(row.getMap("attrs", classOf[String], classOf[java.lang.Long]).isEmpty)
  }

  withSet(setName).test("default skip filters whole records for source and destination failures") {
    _ =>
      createTable("aero_key text PRIMARY KEY, neighbor text, value int, items list<bigint>")
      put(
        "good",
        new Bin("neighbor", "kept"),
        new Bin("value", 42L),
        new Bin("items", List(1L, 2L).asJava)
      )
      put("bad-source", new Bin("neighbor", "must not be written"), new Bin("value", "invalid"))
      put("bad-target", new Bin("neighbor", "must not be written"), new Bin("value", 2147483648L))
      put(
        "bad-collection",
        new Bin("neighbor", "must not be written"),
        new Bin("items", List[Any](1L, null).asJava)
      )
      put(
        "bad-target-collection",
        new Bin("neighbor", "must not be written"),
        new Bin("items", List.empty[Any].asJava)
      )
      migrate("{neighbor: string, value: long, items: 'list<long>'}")
      // Selecting all columns also catches a partial target write containing only the valid neighbor.
      assertEquals(rows.map(_.getString("aero_key")), Seq("good"))
      assertEquals(rows.head.getString("neighbor"), "kept")
      assertEquals(rows.head.getInt("value"), 42)
      assertEquals(
        rows.head.getList("items", classOf[java.lang.Long]).asScala.map(_.longValue()).toSeq,
        Seq(1L, 2L)
      )
  }

  withSet(setName).test(
    "null policy keeps valid neighbors and nulls entire invalid source or target fields"
  ) { _ =>
    createTable(
      "aero_key text PRIMARY KEY, neighbor text, value int, items list<bigint>, attrs map<bigint,bigint>"
    )
    put(
      "good",
      new Bin("neighbor", "good neighbor"),
      new Bin("value", 42L),
      new Bin("items", List(1L).asJava),
      new Bin("attrs", Map(1L -> 2L).asJava)
    )
    put(
      "bad-source",
      new Bin("neighbor", "source neighbor"),
      new Bin("value", "invalid"),
      new Bin("items", List(3L).asJava)
    )
    put(
      "bad-target",
      new Bin("neighbor", "target neighbor"),
      new Bin("value", 2147483648L),
      new Bin("items", List(4L).asJava)
    )
    put(
      "bad-collection",
      new Bin("neighbor", "collection neighbor"),
      new Bin("value", 12L),
      new Bin("items", List[Any](1L, null).asJava),
      new Bin("attrs", Map[String, Any]("invalid" -> 1L).asJava)
    )
    put(
      "bad-target-collection",
      new Bin("neighbor", "empty collection neighbor"),
      new Bin("value", 5L),
      new Bin("items", List.empty[Any].asJava)
    )
    migrate(
      "{neighbor: string, value: long, items: 'list<long>', attrs: 'map<long,long>'}",
      "invalidRecordHandling: \"null\""
    )
    val actual = rows.map(r => r.getString("aero_key") -> r).toMap
    assertEquals(
      actual.keySet,
      Set("good", "bad-source", "bad-target", "bad-collection", "bad-target-collection")
    )
    assertEquals(actual("good").getInt("value"), 42)
    for (
      (key, neighbor, item) <-
        Seq(("bad-source", "source neighbor", 3L), ("bad-target", "target neighbor", 4L))
    ) {
      assertEquals(actual(key).getString("neighbor"), neighbor)
      assert(actual(key).isNull("value"))
      assertEquals(actual(key).getList("items", classOf[java.lang.Long]).get(0).longValue(), item)
    }
    assertEquals(actual("bad-collection").getString("neighbor"), "collection neighbor")
    assertEquals(actual("bad-collection").getInt("value"), 12)
    assert(actual("bad-collection").isNull("items"))
    assert(actual("bad-collection").isNull("attrs"))
    assertEquals(actual("bad-target-collection").getString("neighbor"), "empty collection neighbor")
    assertEquals(actual("bad-target-collection").getInt("value"), 5)
    assert(actual("bad-target-collection").isNull("items"))
  }

  withSet(setName).test(
    "default skip rejects lossy target double conversion without losing valid records"
  ) { _ =>
    createTable("aero_key text PRIMARY KEY, value double")
    put("good", new Bin("value", 42L))
    put("bad", new Bin("value", 9007199254740993L))
    migrate("{value: long}")
    assertEquals(rows.map(_.getString("aero_key")), Seq("good"))
    assertEquals(rows.head.getDouble("value"), 42.0)
  }

  for (handling <- Seq("skip", "null")) {
    for (keyMode <- Seq("string", "integer", "binary"))
      withSet(setName).test(s"$handling skips missing and mismatched stored $keyMode keys") { _ =>
        val cqlType = Map("string" -> "text", "integer" -> "bigint", "binary" -> "blob")(keyMode)
        createTable(s"aero_key $cqlType PRIMARY KEY, value text")
        val validKey = keyMode match {
          case "string"  => new Key(aerospikeNamespace, setName, "valid")
          case "integer" => new Key(aerospikeNamespace, setName, 123L)
          case "binary"  => new Key(aerospikeNamespace, setName, Array[Byte](1, 2))
        }
        val invalidKey =
          if (keyMode == "string") new Key(aerospikeNamespace, setName, 456L)
          else new Key(aerospikeNamespace, setName, "wrong type")
        val policy = new WritePolicy()
        policy.sendKey = true
        retryPut(policy, validKey, new Bin("value", "valid identity"))
        retryPut(policy, invalidKey, new Bin("value", "wrong type"))
        policy.sendKey = false
        retryPut(
          policy,
          new Key(aerospikeNamespace, setName, "unstored"),
          new Bin("value", "missing identity")
        )
        migrate("{value: string}", s"invalidRecordHandling: \"$handling\"", keyMode = keyMode)
        assertEquals(rows.size, 1)
        val row = rows.head
        assertEquals(row.getString("value"), "valid identity")
        keyMode match {
          case "string"  => assertEquals(row.getString("aero_key"), "valid")
          case "integer" => assertEquals(row.getLong("aero_key"), 123L)
          case "binary" =>
            assertEquals(row.getByteBuffer("aero_key"), ByteBuffer.wrap(Array[Byte](1, 2)))
        }
      }

    withSet(setName).test(
      s"$handling skips missing or invalid secondary target keys after chained renames"
    ) { _ =>
      createTable("id text, bucket int, value text, PRIMARY KEY (id, bucket)")
      put("good", new Bin("bucket", 42L), new Bin("value", "valid"))
      put("missing", new Bin("value", "must not be written"))
      put("overflow", new Bin("bucket", 2147483648L), new Bin("value", "must not be written"))
      migrate(
        "{bucket: long, value: string}",
        s"invalidRecordHandling: \"$handling\"",
        """renames:
          |  - {from: aero_key, to: temporary}
          |  - {from: temporary, to: id}""".stripMargin
      )
      assertEquals(rows.size, 1)
      assertEquals(rows.head.getString("id"), "good")
      assertEquals(rows.head.getInt("bucket"), 42)
    }

    for (keyMode <- Seq("string", "binary"))
      withSet(setName).test(
        s"$handling skips empty stored $keyMode keys for a single partition key"
      ) { _ =>
        val cqlType = if (keyMode == "string") "text" else "blob"
        createTable(s"aero_key $cqlType PRIMARY KEY, value text")
        val (validKey, emptyKey) =
          if (keyMode == "string")
            (
              new Key(aerospikeNamespace, setName, "valid"),
              new Key(aerospikeNamespace, setName, "")
            )
          else
            (
              new Key(aerospikeNamespace, setName, Array[Byte](1, 2)),
              new Key(aerospikeNamespace, setName, Array.emptyByteArray)
            )
        val policy = new WritePolicy()
        policy.sendKey = true
        retryPut(policy, validKey, new Bin("value", "valid identity"))
        retryPut(policy, emptyKey, new Bin("value", "must not be written"))
        migrate("{value: string}", s"invalidRecordHandling: \"$handling\"", keyMode = keyMode)
        val actual = rows
        assertEquals(actual.size, 1)
        assertEquals(actual.head.getString("value"), "valid identity")
        if (keyMode == "string") assertEquals(actual.head.getString("aero_key"), "valid")
        else
          assertEquals(actual.head.getByteBuffer("aero_key"), ByteBuffer.wrap(Array[Byte](1, 2)))
      }

    withSet(setName).test(
      s"$handling skips invalid single partition bins while preserving an empty clustering key"
    ) { _ =>
      createTable("bucket text, aero_key text, value text, PRIMARY KEY (bucket, aero_key)")
      put("good", new Bin("bucket", "partition"), new Bin("value", "valid"))
      put("bad-partition", new Bin("bucket", ""), new Bin("value", "must not be written"))
      put("", new Bin("bucket", "partition"), new Bin("value", "valid empty clustering"))
      put("oversized", new Bin("bucket", "x" * 65534), new Bin("value", "must not be written"))
      migrate("{bucket: string, value: string}", s"invalidRecordHandling: \"$handling\"")
      val actual = rows.map(row => row.getString("aero_key") -> row).toMap
      assertEquals(actual.keySet, Set("good", ""))
      assertEquals(actual("good").getString("value"), "valid")
      assertEquals(actual("").getString("bucket"), "partition")
      assertEquals(actual("").getString("value"), "valid empty clustering")
    }

    withSet(setName).test(
      s"$handling skips oversized composite partitions and preserves empty components"
    ) { _ =>
      createTable(
        "aero_key text, bucket text, suffix text, value text, PRIMARY KEY ((aero_key, bucket), suffix)"
      )
      put(
        "good",
        new Bin("bucket", "partition"),
        new Bin("suffix", "suffix"),
        new Bin("value", "valid")
      )
      put(
        "",
        new Bin("bucket", ""),
        new Bin("suffix", ""),
        new Bin("value", "valid empty components")
      )
      // Each component fits separately, but their combined Scylla representation does not.
      put(
        "oversized",
        new Bin("bucket", "b" * 65523),
        new Bin("suffix", "suffix"),
        new Bin("value", "must not be written")
      )
      migrate(
        "{bucket: string, suffix: string, value: string}",
        s"invalidRecordHandling: \"$handling\""
      )
      val actual = rows.map(row => row.getString("aero_key") -> row).toMap
      assertEquals(actual.keySet, Set("good", ""))
      assertEquals(actual("good").getString("value"), "valid")
      assertEquals(actual("").getString("bucket"), "")
      assertEquals(actual("").getString("suffix"), "")
      assertEquals(actual("").getString("value"), "valid empty components")
    }

    withSet(setName).test(s"$handling skips oversized single clustering bins") { _ =>
      createTable("aero_key text, component text, value text, PRIMARY KEY (aero_key, component)")
      put("good", new Bin("component", "clustering"), new Bin("value", "valid"))
      put("empty", new Bin("component", ""), new Bin("value", "valid empty clustering"))
      put("oversized", new Bin("component", "x" * 70000), new Bin("value", "must not be written"))
      migrate("{component: string, value: string}", s"invalidRecordHandling: \"$handling\"")
      val actual = rows.map(row => row.getString("aero_key") -> row).toMap
      assertEquals(actual.keySet, Set("good", "empty"))
      assertEquals(actual("good").getString("component"), "clustering")
      assertEquals(actual("good").getString("value"), "valid")
      assertEquals(actual("empty").getString("component"), "")
      assertEquals(actual("empty").getString("value"), "valid empty clustering")
    }

    withSet(setName).test(s"$handling skips oversized composite clustering bins") { _ =>
      createTable(
        "aero_key text, first text, second text, value text, PRIMARY KEY (aero_key, first, second)"
      )
      put("good", new Bin("first", "one"), new Bin("second", "two"), new Bin("value", "valid"))
      put(
        "empty",
        new Bin("first", ""),
        new Bin("second", ""),
        new Bin("value", "valid empty components")
      )
      // Each component fits separately, but the clustering group exceeds its encoded-size limit.
      put(
        "oversized",
        new Bin("first", "a" * 40000),
        new Bin("second", "b" * 30000),
        new Bin("value", "must not be written")
      )
      migrate(
        "{second: string, value: string, first: string}",
        s"invalidRecordHandling: \"$handling\""
      )
      val actual = rows.map(row => row.getString("aero_key") -> row).toMap
      assertEquals(actual.keySet, Set("good", "empty"))
      assertEquals(actual("good").getString("first"), "one")
      assertEquals(actual("good").getString("second"), "two")
      assertEquals(actual("good").getString("value"), "valid")
      assertEquals(actual("empty").getString("first"), "")
      assertEquals(actual("empty").getString("second"), "")
      assertEquals(actual("empty").getString("value"), "valid empty components")
    }

    withSet(setName).test(s"$handling preserves fatal static target identity errors") { _ =>
      createTable("aero_key bigint PRIMARY KEY, value text")
      put("01", new Bin("value", "distinct string identity"))
      assertRejected("key")(migrate("{value: string}", s"invalidRecordHandling: \"$handling\""))
    }
  }

  withSet(setName).test("generated metadata schema collisions fail before scanning") { _ =>
    createTable("aero_key text PRIMARY KEY, aero_ttl int")
    put("key", new Bin("aero_ttl", 123L))
    assertRejected("aero_ttl")(migrate("{aero_ttl: long}", "preserveTTL: true"))
  }

  withSet(setName).test("ordered maps preserve binary keys with explicit schema") { _ =>
    createTable("aero_key text PRIMARY KEY, attrs map<blob,bigint>")
    val values = new java.util.TreeMap[ByteBuffer, java.lang.Long]()
    values.put(ByteBuffer.wrap(Array[Byte](1, 2)), java.lang.Long.valueOf(11))
    values.put(ByteBuffer.wrap(Array[Byte](3, 4)), java.lang.Long.valueOf(22))
    put("key", new Bin("attrs", values))
    migrate("{attrs: 'map<binary,long>'}")
    assertEquals(
      rows.head.getMap("attrs", classOf[ByteBuffer], classOf[java.lang.Long]).asScala.toMap,
      values.asScala.toMap
    )
  }

  withSet(setName).test("explicit schema preserves sparse bins and omits undeclared data") { _ =>
    createTable("aero_key text PRIMARY KEY, first bigint, second bigint")
    put("first", new Bin("first", 1L), new Bin("undeclared", "ignored"))
    put("second", new Bin("second", 2L))
    migrate("{first: long, second: long}")
    val actual = rows.map(r => r.getString("aero_key") -> r).toMap
    assertEquals(actual.size, 2)
    assertEquals(actual("first").getLong("first"), 1L)
    assert(actual("first").isNull("second"))
    assertEquals(actual("second").getLong("second"), 2L)
    assert(actual("second").isNull("first"))
  }
}
