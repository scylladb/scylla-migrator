package com.scylladb.migrator.writers

import com.scylladb.migrator.alternator.DynamoDBS3ExportE2EBenchmarkUtils.{
  ManifestFile,
  ManifestSummary
}
import com.scylladb.migrator.config.TargetSettings
import com.scylladb.migrator.readers.{ DynamoDBS3Export => DynamoDBS3ExportReader }
import io.circe.parser.{ decode => jsonDecode }
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  KeySchemaElement,
  KeyType,
  ScalarAttributeType,
  TableDescription
}

import java.io.File
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Unit tests for [[DynamoDBS3Export.writeRDD]] manifest generation. */
class DynamoDBS3ExportWriterTest extends munit.FunSuite {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("DynamoDBS3ExportWriterTest")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  private val tempDir: FunFixture[File] = FunFixture(
    setup = _ =>
      new File(System.getProperty("java.io.tmpdir"), s"s3export-test-${System.nanoTime()}"),
    teardown = dir =>
      if (dir.exists()) com.scylladb.migrator.TestFileUtils.deleteRecursive(dir)
  )

  /** Build an RDD of (Text, DynamoDBItemWritable) from serializable data. */
  private def makeRDD(count: Int, partitions: Int) = {
    // Parallelize plain indices (Int is serializable), then create Hadoop Writables inside mapPartitions
    spark.sparkContext
      .parallelize(0 until count, partitions)
      .mapPartitions { iter =>
        iter.map { i =>
          val writable = new DynamoDBItemWritable()
          writable.setItem(
            new java.util.HashMap[String, AttributeValue](
              Map(
                "id"   -> AttributeValue.fromS(s"id-$i"),
                "col1" -> AttributeValue.fromS(s"value-$i")
              ).asJava
            )
          )
          (new Text(), writable)
        }
      }
  }

  tempDir.test("writeRDD produces manifest-summary.json with correct item count") { outputDir =>
    val rdd = makeRDD(10, 2)
    val target = TargetSettings.DynamoDBS3Export(outputDir.getAbsolutePath)
    DynamoDBS3Export.writeRDD(target, rdd)(spark)

    // Verify manifest-summary.json
    val summaryFile = new File(outputDir, "manifest-summary.json")
    assert(summaryFile.exists(), "manifest-summary.json should exist")
    val summaryJson = Using.resource(Source.fromFile(summaryFile))(_.mkString)
    val summary = jsonDecode[ManifestSummary](summaryJson).fold(throw _, identity)
    assertEquals(summary.itemCount, 10L)
    assertEquals(summary.outputFormat, "DYNAMODB_JSON")
    assertEquals(summary.manifestFilesS3Key, "manifest-files.json")

    // Verify manifest-files.json
    val manifestFilesFile = new File(outputDir, "manifest-files.json")
    assert(manifestFilesFile.exists(), "manifest-files.json should exist")
    val manifestLines = Using.resource(Source.fromFile(manifestFilesFile))(_.getLines().toList)
    assert(manifestLines.nonEmpty, "manifest-files.json should have entries")

    var totalFromManifest = 0L
    for (line <- manifestLines) {
      val mf = jsonDecode[ManifestFile](line).fold(throw _, identity)
      assert(
        mf.dataFileS3Key.startsWith("data/"),
        s"dataFileS3Key should start with data/: ${mf.dataFileS3Key}"
      )
      assert(
        mf.dataFileS3Key.endsWith(".json.gz"),
        s"dataFileS3Key should end with .json.gz: ${mf.dataFileS3Key}"
      )
      totalFromManifest += mf.itemCount
    }
    assertEquals(
      totalFromManifest,
      10L,
      "Sum of item counts in manifest files should match total"
    )

    // Verify data files are valid gzipped JSON lines
    val dataDir = new File(outputDir, "data")
    assert(dataDir.exists() && dataDir.isDirectory, "data/ directory should exist")
    val dataFiles = dataDir.listFiles().filter(_.getName.endsWith(".json.gz"))
    assert(dataFiles.nonEmpty, "Should have gzipped data files")

    var totalDataItems = 0
    for (f <- dataFiles) {
      val lines = Using.resource(
        Source.fromInputStream(new GZIPInputStream(new java.io.FileInputStream(f)))
      )(_.getLines().toList)
      for (line <- lines) {
        val parsed = io.circe.parser.parse(line).fold(throw _, identity)
        assert(
          parsed.hcursor.downField("Item").focus.isDefined,
          s"Each line should have an Item field"
        )
      }
      totalDataItems += lines.size
    }
    assertEquals(totalDataItems, 10, "Total items across data files should be 10")
  }

  tempDir.test("writer output can be parsed by reader's item decoder (round-trip)") { outputDir =>
    val rdd = makeRDD(10, 2)
    val target = TargetSettings.DynamoDBS3Export(outputDir.getAbsolutePath)
    DynamoDBS3Export.writeRDD(target, rdd)(spark)

    // Read data files and parse each line with the reader's itemDecoder
    val dataDir = new File(outputDir, "data")
    val dataFiles = dataDir.listFiles().filter(_.getName.endsWith(".json.gz"))
    var totalItems = 0
    for (f <- dataFiles) {
      val lines = Using.resource(
        Source.fromInputStream(new GZIPInputStream(new java.io.FileInputStream(f)))
      )(_.getLines().toList)
      for (line <- lines) {
        val decoded = io.circe.parser.decode(line)(DynamoDBS3ExportReader.itemDecoder)
        assert(decoded.isRight, s"Reader failed to parse writer output: ${decoded.left.getOrElse("")}")
        val item = decoded.toOption.get
        assert(item.contains("id"), "Round-trip item should contain 'id'")
        assert(item.contains("col1"), "Round-trip item should contain 'col1'")
        totalItems += 1
      }
    }
    assertEquals(totalItems, 10, "Round-trip should preserve all items")

    // Verify manifest-summary.json can be parsed by reader's decoder
    val summaryFile = new File(outputDir, "manifest-summary.json")
    val summaryJson = Using.resource(Source.fromFile(summaryFile))(_.mkString)
    val readerSummary = jsonDecode(summaryJson)(DynamoDBS3ExportReader.ManifestSummary.jsonDecoder)
    assert(readerSummary.isRight, s"Reader failed to parse manifest-summary: ${readerSummary.left.getOrElse("")}")
  }

  tempDir.test("writeRDD handles more partitions than rows (empty partitions)") { outputDir =>
    val rdd = makeRDD(2, 5) // 2 rows across 5 partitions — 3 will be empty
    val target = TargetSettings.DynamoDBS3Export(outputDir.getAbsolutePath)
    DynamoDBS3Export.writeRDD(target, rdd)(spark)

    val summaryFile = new File(outputDir, "manifest-summary.json")
    assert(summaryFile.exists(), "manifest-summary.json should exist")
    val summaryJson = Using.resource(Source.fromFile(summaryFile))(_.mkString)
    val summary = jsonDecode[ManifestSummary](summaryJson).fold(throw _, identity)
    assertEquals(summary.itemCount, 2L, "Item count should be 2 even with empty partitions")

    val manifestFilesFile = new File(outputDir, "manifest-files.json")
    val manifestLines = Using.resource(Source.fromFile(manifestFilesFile))(_.getLines().toList)
    val totalFromManifest = manifestLines.map { line =>
      jsonDecode[ManifestFile](line).fold(throw _, identity).itemCount
    }.sum
    assertEquals(totalFromManifest, 2L, "Manifest file item counts should sum to 2")
  }

  tempDir.test("writeRDD with empty RDD produces valid manifests with itemCount 0") { outputDir =>
    val rdd = makeRDD(0, 2)
    val target = TargetSettings.DynamoDBS3Export(outputDir.getAbsolutePath)
    DynamoDBS3Export.writeRDD(target, rdd)(spark)

    val summaryFile = new File(outputDir, "manifest-summary.json")
    assert(summaryFile.exists(), "manifest-summary.json should exist")
    val summaryJson = Using.resource(Source.fromFile(summaryFile))(_.mkString)
    val summary = jsonDecode[ManifestSummary](summaryJson).fold(throw _, identity)
    assertEquals(summary.itemCount, 0L, "Item count should be 0 for empty RDD")
    assertEquals(summary.outputFormat, "DYNAMODB_JSON")

    val manifestFilesFile = new File(outputDir, "manifest-files.json")
    assert(manifestFilesFile.exists(), "manifest-files.json should exist")
    val manifestLines = Using.resource(Source.fromFile(manifestFilesFile))(_.getLines().toList)
    val totalFromManifest = manifestLines.map { line =>
      jsonDecode[ManifestFile](line).fold(throw _, identity).itemCount
    }.sum
    assertEquals(totalFromManifest, 0L, "Manifest file item counts should sum to 0")
  }

  tempDir.test("writeRDD with tableDescription includes key schema in manifest summary") { outputDir =>
    val rdd = makeRDD(5, 1)
    val target = TargetSettings.DynamoDBS3Export(outputDir.getAbsolutePath)

    val tableDesc = TableDescription
      .builder()
      .keySchema(
        KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build(),
        KeySchemaElement.builder().attributeName("sort").keyType(KeyType.RANGE).build()
      )
      .attributeDefinitions(
        AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build(),
        AttributeDefinition.builder().attributeName("sort").attributeType(ScalarAttributeType.N).build()
      )
      .build()

    DynamoDBS3Export.writeRDD(target, rdd, Some(tableDesc))(spark)

    val summaryFile = new File(outputDir, "manifest-summary.json")
    val summaryJson = Using.resource(Source.fromFile(summaryFile))(_.mkString)
    val parsed = io.circe.parser.parse(summaryJson).fold(throw _, identity)
    val cursor = parsed.hcursor

    // Verify tableKeySchema
    val keySchema = cursor.downField("tableKeySchema").as[List[io.circe.Json]].fold(throw _, identity)
    assertEquals(keySchema.size, 2, "Should have 2 key schema elements")
    val hashKey = keySchema.head.hcursor
    assertEquals(hashKey.get[String]("AttributeName").toOption, Some("id"))
    assertEquals(hashKey.get[String]("KeyType").toOption, Some("HASH"))
    val rangeKey = keySchema(1).hcursor
    assertEquals(rangeKey.get[String]("AttributeName").toOption, Some("sort"))
    assertEquals(rangeKey.get[String]("KeyType").toOption, Some("RANGE"))

    // Verify tableAttributeDefinitions
    val attrDefs = cursor.downField("tableAttributeDefinitions").as[List[io.circe.Json]].fold(throw _, identity)
    assertEquals(attrDefs.size, 2, "Should have 2 attribute definitions")
    val idAttr = attrDefs.head.hcursor
    assertEquals(idAttr.get[String]("AttributeName").toOption, Some("id"))
    assertEquals(idAttr.get[String]("AttributeType").toOption, Some("S"))
    val sortAttr = attrDefs(1).hcursor
    assertEquals(sortAttr.get[String]("AttributeName").toOption, Some("sort"))
    assertEquals(sortAttr.get[String]("AttributeType").toOption, Some("N"))

    // Basic fields should still be present
    assertEquals(cursor.get[Long]("itemCount").toOption, Some(5L))
    assertEquals(cursor.get[String]("outputFormat").toOption, Some("DYNAMODB_JSON"))
  }
}
