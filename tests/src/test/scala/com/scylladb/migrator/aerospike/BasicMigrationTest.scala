package com.scylladb.migrator.aerospike

import com.aerospike.client.{ Bin, Key }
import com.aerospike.client.policy.WritePolicy
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.{ QueryBuilder, SchemaBuilder }
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration

import scala.jdk.CollectionConverters._
import scala.util.chaining._

class BasicMigrationTest extends MigratorSuite {

  // Set names per config file group — must match the 'set' and 'table' values in the
  // corresponding YAML configs under tests/src/test/configurations/aerospike-to-scylla-*.yaml.
  // Multiple tests sharing the same set name is safe because: (1) parallelExecution := false
  // ensures sequential execution, and (2) withSet() truncates the set and calls waitForTruncate()
  // which polls until the async Aerospike truncation completes before each test.
  private val BasicSetName = "basictest" // -> aerospike-to-scylla-basic.yaml
  private val BinsFilterSetName = "binsfilter" // -> aerospike-to-scylla-bins-filter.yaml
  private val SchemaSetName = "schematest" // -> aerospike-to-scylla-schema-override.yaml
  private val BinsSchemaSetName = "binsschema" // -> aerospike-to-scylla-bins-schema.yaml
  private val MetadataSetName = "metatest" // -> aerospike-to-scylla-metadata.yaml

  withSet(BasicSetName).test("Aerospike: basic migration to Scylla") { setName =>
    // Create target table in Scylla matching the discovered Aerospike schema.
    // Aerospike returns all integers as Long, so we use BIGINT.
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("foo", DataTypes.TEXT)
      .withColumn("bar", DataTypes.BIGINT)
      .build()
    targetScylla().execute(createStmt)

    // Insert data into Aerospike
    val key = new Key(aerospikeNamespace, setName, "12345")
    val policy = new WritePolicy()
    policy.sendKey = true
    retryPut(
      policy,
      key,
      new Bin("foo", "hello"),
      new Bin("bar", 42)
    )

    // Run migration
    successfullyPerformMigration("aerospike-to-scylla-basic.yaml")

    // Verify in Scylla
    val selectStmt = QueryBuilder.selectFrom(keyspace, setName).all().build()
    targetScylla().execute(selectStmt).tap { resultSet =>
      val rows = resultSet.all().asScala
      assertEquals(rows.size, 1)
      val row = rows.head
      assertEquals(row.getString("aero_key"), "12345")
      assertEquals(row.getString("foo"), "hello")
      assertEquals(row.getLong("bar"), 42L)
    }
  }

  withSet(BasicSetName).test("Aerospike: migration with multiple records") { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("name", DataTypes.TEXT)
      .withColumn("age", DataTypes.BIGINT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true

    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "user1"),
      new Bin("name", "Alice"),
      new Bin("age", 30)
    )
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "user2"),
      new Bin("name", "Bob"),
      new Bin("age", 25)
    )
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "user3"),
      new Bin("name", "Charlie"),
      new Bin("age", 35)
    )

    successfullyPerformMigration("aerospike-to-scylla-basic.yaml")

    val selectStmt = QueryBuilder.selectFrom(keyspace, setName).all().build()
    targetScylla().execute(selectStmt).tap { resultSet =>
      val rows = resultSet.all().asScala
      assertEquals(rows.size, 3)

      val rowMap = rows.map(r => r.getString("aero_key") -> r).toMap
      assertEquals(rowMap("user1").getString("name"), "Alice")
      assertEquals(rowMap("user1").getLong("age"), 30L)
      assertEquals(rowMap("user2").getString("name"), "Bob")
      assertEquals(rowMap("user2").getLong("age"), 25L)
      assertEquals(rowMap("user3").getString("name"), "Charlie")
      assertEquals(rowMap("user3").getLong("age"), 35L)
    }
  }

  withSet(BinsFilterSetName).test("Aerospike: migration with bins filter") { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("foo", DataTypes.TEXT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "key1"),
      new Bin("foo", "included"),
      new Bin("bar", 99)
    )

    successfullyPerformMigration("aerospike-to-scylla-bins-filter.yaml")

    val rows = targetScylla()
      .execute(
        QueryBuilder.selectFrom(keyspace, setName).all().build()
      )
      .all()
      .asScala
    assertEquals(rows.size, 1)
    assertEquals(rows.head.getString("foo"), "included")
    assertEquals(rows.head.getColumnDefinitions.contains("bar"), false)
  }

  withSet(BasicSetName).test("Aerospike: migration with digest key (sendKey=false)") { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("val", DataTypes.TEXT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy() // sendKey defaults to false
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "invisible-key"),
      new Bin("val", "data")
    )

    successfullyPerformMigration("aerospike-to-scylla-basic.yaml")

    val rows = targetScylla()
      .execute(
        QueryBuilder.selectFrom(keyspace, setName).all().build()
      )
      .all()
      .asScala
    assertEquals(rows.size, 1)
    val aeroKey = rows.head.getString("aero_key")
    assert(aeroKey != "invisible-key", "Expected digest key, got userKey")
    assert(aeroKey.matches("[0-9a-f]+"), s"Expected hex digest, got: $aeroKey")
  }

  withSet(BasicSetName).test("Aerospike: type conflict falls back to StringType") { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("data", DataTypes.TEXT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true

    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "row1"),
      new Bin("data", 42L)
    )
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "row2"),
      new Bin("data", "hello")
    )

    successfullyPerformMigration("aerospike-to-scylla-basic.yaml")

    val rows = targetScylla()
      .execute(
        QueryBuilder.selectFrom(keyspace, setName).all().build()
      )
      .all()
      .asScala
    assertEquals(rows.size, 2)
    val rowMap = rows.map(r => r.getString("aero_key") -> r.getString("data")).toMap
    assertEquals(rowMap("row1"), "42")
    assertEquals(rowMap("row2"), "hello")
  }

  withSet(SchemaSetName).test(
    "Aerospike: migration with explicit schema override (no discovery)"
  ) { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("foo", DataTypes.TEXT)
      .withColumn("bar", DataTypes.BIGINT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "key1"),
      new Bin("foo", "hello"),
      new Bin("bar", 42)
    )

    successfullyPerformMigration("aerospike-to-scylla-schema-override.yaml")

    val rows = targetScylla()
      .execute(QueryBuilder.selectFrom(keyspace, setName).all().build())
      .all()
      .asScala
    assertEquals(rows.size, 1)
    assertEquals(rows.head.getString("aero_key"), "key1")
    assertEquals(rows.head.getString("foo"), "hello")
    assertEquals(rows.head.getLong("bar"), 42L)
  }

  withSet(BinsSchemaSetName).test(
    "Aerospike: migration with both bins filter and schema override"
  ) { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("foo", DataTypes.TEXT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "key1"),
      new Bin("foo", "included"),
      new Bin("bar", 42),
      new Bin("baz", "excluded")
    )

    successfullyPerformMigration("aerospike-to-scylla-bins-schema.yaml")

    val rows = targetScylla()
      .execute(QueryBuilder.selectFrom(keyspace, setName).all().build())
      .all()
      .asScala
    assertEquals(rows.size, 1)
    assertEquals(rows.head.getString("aero_key"), "key1")
    assertEquals(rows.head.getString("foo"), "included")
    assertEquals(rows.head.getColumnDefinitions.contains("bar"), false)
    assertEquals(rows.head.getColumnDefinitions.contains("baz"), false)
  }

  withSet(MetadataSetName).test("Aerospike: migration preserves TTL") { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("val", DataTypes.TEXT)
      .withColumn("aero_ttl", DataTypes.INT)
      .withColumn("aero_generation", DataTypes.INT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey    = true
    policy.expiration = 3600 // 1 hour TTL
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "ttl-key"),
      new Bin("val", "with-ttl")
    )

    successfullyPerformMigration("aerospike-to-scylla-metadata.yaml")

    val rows = targetScylla()
      .execute(QueryBuilder.selectFrom(keyspace, setName).all().build())
      .all()
      .asScala
    assertEquals(rows.size, 1)
    assertEquals(rows.head.getString("aero_key"), "ttl-key")
    assertEquals(rows.head.getString("val"), "with-ttl")
    // TTL should be a positive value less than or equal to 3600 (may have ticked down slightly)
    val ttl = rows.head.getInt("aero_ttl")
    assert(ttl > 0 && ttl <= 3600, s"Expected TTL in (0, 3600], got: $ttl")
    // Generation should be 1 for a freshly written record
    val gen = rows.head.getInt("aero_generation")
    assertEquals(gen, 1)
  }

  withSet(MetadataSetName).test("Aerospike: TTL is -1 for records with no expiration") { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("val", DataTypes.TEXT)
      .withColumn("aero_ttl", DataTypes.INT)
      .withColumn("aero_generation", DataTypes.INT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true
    // Default expiration = 0 means use namespace default; for test namespace this is no expiration
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "no-ttl"),
      new Bin("val", "forever")
    )

    successfullyPerformMigration("aerospike-to-scylla-metadata.yaml")

    val rows = targetScylla()
      .execute(QueryBuilder.selectFrom(keyspace, setName).all().build())
      .all()
      .asScala
    assertEquals(rows.size, 1)
    val ttl = rows.head.getInt("aero_ttl")
    assertEquals(ttl, -1, "Records with no expiration should have TTL = -1")
  }

  withSet(BasicSetName).test("Aerospike: migration with list and map bins") { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("tags", DataTypes.frozenListOf(DataTypes.TEXT))
      .withColumn("attrs", DataTypes.frozenMapOf(DataTypes.TEXT, DataTypes.TEXT))
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true

    val tags = java.util.Arrays.asList("a", "b", "c")
    val attrs = new java.util.HashMap[String, String]()
    attrs.put("color", "red")

    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "key1"),
      new Bin("tags", tags),
      new Bin("attrs", attrs)
    )

    successfullyPerformMigration("aerospike-to-scylla-basic.yaml")

    val rows = targetScylla()
      .execute(QueryBuilder.selectFrom(keyspace, setName).all().build())
      .all()
      .asScala
    assertEquals(rows.size, 1)
    assertEquals(rows.head.getList("tags", classOf[String]).asScala.toList, List("a", "b", "c"))
    assertEquals(
      rows.head.getMap("attrs", classOf[String], classOf[String]).asScala.toMap,
      Map("color" -> "red")
    )
  }

  withSet(BasicSetName).test("Aerospike: migration with Long key type") { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.BIGINT)
      .withColumn("val", DataTypes.TEXT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, 12345L),
      new Bin("val", "data")
    )

    successfullyPerformMigration("aerospike-to-scylla-basic.yaml")

    val rows = targetScylla()
      .execute(QueryBuilder.selectFrom(keyspace, setName).all().build())
      .all()
      .asScala
    assertEquals(rows.size, 1)
    assertEquals(rows.head.getLong("aero_key"), 12345L)
    assertEquals(rows.head.getString("val"), "data")
  }

  withSet(MetadataSetName).test("Aerospike: generation increments on update") { setName =>
    // Table must include aero_ttl because the metadata config has preserveTTL: true
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("val", DataTypes.TEXT)
      .withColumn("aero_ttl", DataTypes.INT)
      .withColumn("aero_generation", DataTypes.INT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true
    val key = new Key(aerospikeNamespace, setName, "gen-key")
    // Write twice to increment generation
    retryPut(policy, key, new Bin("val", "v1"))
    retryPut(policy, key, new Bin("val", "v2"))

    successfullyPerformMigration("aerospike-to-scylla-metadata.yaml")

    val rows = targetScylla()
      .execute(QueryBuilder.selectFrom(keyspace, setName).all().build())
      .all()
      .asScala
    assertEquals(rows.size, 1)
    assertEquals(rows.head.getString("val"), "v2")
    val gen = rows.head.getInt("aero_generation")
    assertEquals(gen, 2, "Generation should be 2 after two writes")
  }

  withSet(BasicSetName).test("Aerospike: migration with schemaSampleSize=1") { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("foo", DataTypes.TEXT)
      .withColumn("bar", DataTypes.BIGINT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "key1"),
      new Bin("foo", "hello"),
      new Bin("bar", 42)
    )

    // schemaSampleSize=1 limits schema discovery to a single record — should still work
    successfullyPerformMigration("aerospike-to-scylla-sample-size.yaml")

    val rows = targetScylla()
      .execute(QueryBuilder.selectFrom(keyspace, setName).all().build())
      .all()
      .asScala
    assertEquals(rows.size, 1)
    assertEquals(rows.head.getString("aero_key"), "key1")
  }

  withSet(BasicSetName).test("Aerospike: migration handles null/missing bins") { setName =>
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .withColumn("foo", DataTypes.TEXT)
      .withColumn("bar", DataTypes.BIGINT)
      .build()
    targetScylla().execute(createStmt)

    val policy = new WritePolicy()
    policy.sendKey = true

    // First record has both bins
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "full"),
      new Bin("foo", "hello"),
      new Bin("bar", 42)
    )
    // Second record is missing "bar" — will be null after migration
    retryPut(
      policy,
      new Key(aerospikeNamespace, setName, "partial"),
      new Bin("foo", "world")
    )

    successfullyPerformMigration("aerospike-to-scylla-basic.yaml")

    val rows = targetScylla()
      .execute(QueryBuilder.selectFrom(keyspace, setName).all().build())
      .all()
      .asScala
    assertEquals(rows.size, 2)
    val rowMap = rows.map(r => r.getString("aero_key") -> r).toMap
    assertEquals(rowMap("full").getString("foo"), "hello")
    assertEquals(rowMap("full").getLong("bar"), 42L)
    assertEquals(rowMap("partial").getString("foo"), "world")
    assert(rowMap("partial").isNull("bar"), "Expected null for missing bin 'bar'")
  }

  withSet(BasicSetName).test("Aerospike: migration of empty set produces zero rows") { setName =>
    // Create target table with explicit schema since auto-discovery finds no bins
    val createStmt = SchemaBuilder
      .createTable(keyspace, setName)
      .withPartitionKey("aero_key", DataTypes.TEXT)
      .build()
    targetScylla().execute(createStmt)

    // No data inserted — set is empty
    successfullyPerformMigration("aerospike-to-scylla-basic.yaml")

    val rows = targetScylla()
      .execute(QueryBuilder.selectFrom(keyspace, setName).all().build())
      .all()
      .asScala
    assertEquals(rows.size, 0)
  }

}
