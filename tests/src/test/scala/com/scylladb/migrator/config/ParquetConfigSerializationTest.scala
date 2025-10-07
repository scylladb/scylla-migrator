package com.scylladb.migrator.config

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

class ParquetConfigSerializationTest extends munit.FunSuite {

  test("skipParquetFiles serialization to YAML") {
    val config = MigratorConfig(
      source = SourceSettings.Parquet(
        path = "s3a://test-bucket/data/",
        credentials = None,
        region = None,
        endpoint = None
      ),
      target = TargetSettings.Scylla(
        host = "scylla-server",
        port = 9042,
        localDC = Some("datacenter1"),
        credentials = None,
        sslOptions = None,
        keyspace = "test_keyspace",
        table = "test_table",
        connections = Some(16),
        stripTrailingZerosForDecimals = false,
        writeTTLInS = None,
        writeWritetimestampInuS = None,
        consistencyLevel = "LOCAL_QUORUM"
      ),
      renames = None,
      savepoints = Savepoints(300, "/app/savepoints"),
      skipTokenRanges = None,
      skipSegments = None,
      skipParquetFiles = Some(Set(
        "s3a://test-bucket/data/part-00001.parquet",
        "s3a://test-bucket/data/part-00002.parquet",
        "s3a://test-bucket/data/part-00003.parquet"
      )),
      validation = None
    )

    val yaml = config.render
    
    assert(yaml.contains("skipParquetFiles"))
    assert(yaml.contains("part-00001.parquet"))
    assert(yaml.contains("part-00002.parquet"))
    assert(yaml.contains("part-00003.parquet"))
  }

  test("skipParquetFiles deserialization from YAML") {
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

skipParquetFiles:
  - "s3a://test-bucket/data/part-00001.parquet"
  - "s3a://test-bucket/data/part-00002.parquet"

skipTokenRanges: null
skipSegments: null
renames: null
validation: null
"""

    val tempFile = Files.createTempFile("test-config", ".yaml")
    try {
      Files.write(tempFile, yamlContent.getBytes(StandardCharsets.UTF_8))
      
      val config = MigratorConfig.loadFrom(tempFile.toString)
      
      assertEquals(config.skipParquetFiles.isDefined, true)
      assertEquals(config.skipParquetFiles.get.size, 2)
      assert(config.skipParquetFiles.get.contains("s3a://test-bucket/data/part-00001.parquet"))
      assert(config.skipParquetFiles.get.contains("s3a://test-bucket/data/part-00002.parquet"))
      
      assertEquals(config.getSkipParquetFilesOrEmptySet.size, 2)
      
    } finally {
      Files.delete(tempFile)
    }
  }

  test("skipParquetFiles round-trip serialization") {
    val originalFiles = Set(
      "s3a://bucket/file1.parquet",
      "file:///local/file2.parquet",
      "hdfs://namenode/file3.parquet"
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
      savepoints = Savepoints(300, "/tmp"),
      skipTokenRanges = None,
      skipSegments = None,
      skipParquetFiles = Some(originalFiles),
      validation = None
    )

    val yaml = config1.render
    
    val tempFile = Files.createTempFile("roundtrip-config", ".yaml")
    try {
      Files.write(tempFile, yaml.getBytes(StandardCharsets.UTF_8))
      val config2 = MigratorConfig.loadFrom(tempFile.toString)
      
      assertEquals(config2.skipParquetFiles.get, originalFiles)
    } finally {
      Files.delete(tempFile)
    }
  }
}
