package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{MigratorConfig, Savepoints, SourceSettings, TargetSettings}
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

class ParquetModeSelectionTest extends munit.FunSuite {

  test("savepoints.enableParquetFileTracking defaults to true when not specified") {
    val yamlContent = """
source:
  type: parquet
  path: "s3a://test-bucket/data/"

target:
  type: scylla
  host: scylla-server
  port: 9042
  keyspace: test_keyspace
  table: test_table
  localDC: datacenter1
  consistencyLevel: LOCAL_QUORUM
  connections: 16
  stripTrailingZerosForDecimals: false

savepoints:
  path: /app/savepoints
  intervalSeconds: 300
"""

    val tempFile = Files.createTempFile("test-config-default", ".yaml")
    try {
      Files.write(tempFile, yamlContent.getBytes(StandardCharsets.UTF_8))

      val config = MigratorConfig.loadFrom(tempFile.toString)

      assertEquals(config.savepoints.enableParquetFileTracking, true,
        "enableParquetFileTracking should default to true")

    } finally {
      Files.delete(tempFile)
    }
  }

  test("savepoints.enableParquetFileTracking can be explicitly set to true") {
    val yamlContent = """
source:
  type: parquet
  path: "s3a://test-bucket/data/"

target:
  type: scylla
  host: scylla-server
  port: 9042
  keyspace: test_keyspace
  table: test_table
  localDC: datacenter1
  consistencyLevel: LOCAL_QUORUM
  connections: 16
  stripTrailingZerosForDecimals: false

savepoints:
  path: /app/savepoints
  intervalSeconds: 300
  enableParquetFileTracking: true
"""

    val tempFile = Files.createTempFile("test-config-new-mode", ".yaml")
    try {
      Files.write(tempFile, yamlContent.getBytes(StandardCharsets.UTF_8))

      val config = MigratorConfig.loadFrom(tempFile.toString)

      assertEquals(config.savepoints.enableParquetFileTracking, true,
        "enableParquetFileTracking should be true when explicitly set")

    } finally {
      Files.delete(tempFile)
    }
  }

  test("savepoints.enableParquetFileTracking can be set to false for legacy mode") {
    val yamlContent = """
source:
  type: parquet
  path: "s3a://test-bucket/data/"

target:
  type: scylla
  host: scylla-server
  port: 9042
  keyspace: test_keyspace
  table: test_table
  localDC: datacenter1
  consistencyLevel: LOCAL_QUORUM
  connections: 16
  stripTrailingZerosForDecimals: false

savepoints:
  path: /app/savepoints
  intervalSeconds: 300
  enableParquetFileTracking: false
"""

    val tempFile = Files.createTempFile("test-config-legacy-mode", ".yaml")
    try {
      Files.write(tempFile, yamlContent.getBytes(StandardCharsets.UTF_8))

      val config = MigratorConfig.loadFrom(tempFile.toString)

      assertEquals(config.savepoints.enableParquetFileTracking, false,
        "enableParquetFileTracking should be false when explicitly set")

    } finally {
      Files.delete(tempFile)
    }
  }

  test("savepoints configuration round-trip with enableParquetFileTracking") {
    val savepoints1 = Savepoints(
      intervalSeconds = 60,
      path = "/tmp/savepoints",
      enableParquetFileTracking = false
    )

    val config1 = MigratorConfig(
      source = SourceSettings.Parquet("s3a://bucket/", None, None, None),
      target = TargetSettings.Scylla(
        host = "localhost",
        port = 9042,
        localDC = Some("dc1"),
        credentials = None,
        sslOptions = None,
        keyspace = "ks",
        table = "tbl",
        connections = Some(8),
        stripTrailingZerosForDecimals = false,
        writeTTLInS = None,
        writeWritetimestampInuS = None,
        consistencyLevel = "LOCAL_QUORUM"
      ),
      renames = None,
      savepoints = savepoints1,
      skipTokenRanges = None,
      skipSegments = None,
      skipParquetFiles = None,
      validation = None
    )

    val yaml = config1.render

    val tempFile = Files.createTempFile("roundtrip-savepoints", ".yaml")
    try {
      Files.write(tempFile, yaml.getBytes(StandardCharsets.UTF_8))
      val config2 = MigratorConfig.loadFrom(tempFile.toString)

      assertEquals(config2.savepoints.intervalSeconds, savepoints1.intervalSeconds)
      assertEquals(config2.savepoints.path, savepoints1.path)
      assertEquals(config2.savepoints.enableParquetFileTracking, savepoints1.enableParquetFileTracking,
        "enableParquetFileTracking should survive round-trip serialization")
    } finally {
      Files.delete(tempFile)
    }
  }
}
