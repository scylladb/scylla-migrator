package com.scylladb.migrator.writers

import com.scylladb.migrator.config.TargetSettings
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.SparkSession

import java.io.File

/** Unit tests for [[Parquet.writeDataframe]]. */
class ParquetWriterTest extends munit.FunSuite {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("ParquetWriterTest")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  private val tempDir: FunFixture[File] = FunFixture(
    setup = _ =>
      new File(System.getProperty("java.io.tmpdir"), s"parquet-test-${System.nanoTime()}"),
    teardown = dir =>
      if (dir.exists()) com.scylladb.migrator.TestFileUtils.deleteRecursive(dir)
  )

  tempDir.test("writeDataframe writes Parquet files with correct row count") { outputDir =>
    import spark.implicits._
    val df = (0 until 100).map(i => (s"id-$i", s"value-$i", i)).toDF("id", "col1", "col2")

    val target = TargetSettings.Parquet(outputDir.getAbsolutePath, "gzip")
    Parquet.writeDataframe(target, df)

    val readBack = spark.read.parquet(outputDir.getAbsolutePath)
    assertEquals(readBack.count(), 100L)
    assertEquals(readBack.columns.toSet, Set("id", "col1", "col2"))

    // Verify gzip compression is actually used
    val parquetFile = outputDir.listFiles().find(_.getName.endsWith(".parquet"))
      .getOrElse(fail(s"No .parquet file found in ${outputDir.getAbsolutePath}"))
    val conf = new Configuration()
    val reader = ParquetFileReader.open(
      HadoopInputFile.fromPath(new Path(parquetFile.getAbsolutePath), conf)
    )
    try {
      val codec = reader.getFooter.getBlocks.get(0).getColumns.get(0).getCodec
      assertEquals(codec.name(), "GZIP", "Expected gzip compression codec")
    } finally reader.close()
  }

  tempDir.test("writeDataframe with overwrite mode succeeds when path exists") { outputDir =>
    import spark.implicits._

    // Write initial data
    val df1 = (0 until 50).map(i => (s"id-$i", i)).toDF("id", "col1")
    val target = TargetSettings.Parquet(outputDir.getAbsolutePath, "gzip", "overwrite")
    Parquet.writeDataframe(target, df1)
    assertEquals(spark.read.parquet(outputDir.getAbsolutePath).count(), 50L)

    // Overwrite with different data
    val df2 = (0 until 30).map(i => (s"id-$i", i)).toDF("id", "col1")
    Parquet.writeDataframe(target, df2)
    assertEquals(spark.read.parquet(outputDir.getAbsolutePath).count(), 30L)
  }

  tempDir.test("writeDataframe with error mode fails when path exists") { outputDir =>
    import spark.implicits._
    val df = (0 until 10).map(i => (s"id-$i", i)).toDF("id", "col1")

    val target = TargetSettings.Parquet(outputDir.getAbsolutePath, "gzip", "error")
    Parquet.writeDataframe(target, df)

    // Writing again should fail
    intercept[RuntimeException] {
      Parquet.writeDataframe(target, df)
    }
  }
}
