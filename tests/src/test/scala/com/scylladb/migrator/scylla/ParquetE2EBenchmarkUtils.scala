package com.scylladb.migrator.scylla

import com.scylladb.migrator.TestFileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.io.ColumnIOFactory

import java.io.File
import scala.jdk.CollectionConverters._

object ParquetE2EBenchmarkUtils {
  // Host path maps to /app/parquet/bench_e2e inside the Spark container.
  // Relative to the sbt working directory (tests/).
  val parquetHostDir = new File("docker/parquet/bench_e2e")

  def deleteParquetDir(): Unit =
    if (parquetHostDir.exists())
      TestFileUtils.deleteRecursive(parquetHostDir)

  def countParquetFiles(): Int = countParquetFilesIn(parquetHostDir)

  def countParquetFilesIn(dir: File): Int =
    if (dir.exists()) {
      val files = dir.listFiles()
      if (files != null) files.count(_.getName.endsWith(".parquet"))
      else 0
    } else 0

  /** Count total rows across all Parquet files by reading file footers. */
  def countParquetRows(): Long = countParquetRowsIn(parquetHostDir)

  /** Count total rows across all Parquet files in a given directory. */
  def countParquetRowsIn(dir: File): Long =
    if (dir.exists()) {
      val conf = new Configuration()
      val files = dir.listFiles()
      if (files == null) 0L
      else
        files
          .filter(_.getName.endsWith(".parquet"))
          .map { f =>
            val reader = ParquetFileReader.open(
              HadoopInputFile.fromPath(new Path(f.getAbsolutePath), conf)
            )
            try reader.getFooter.getBlocks.stream().mapToLong(_.getRowCount).sum()
            finally reader.close()
          }
          .sum
    } else 0L

  /** Read the first N rows from Parquet files in the given directory.
    * Returns a list of maps (column name -> string value).
    */
  def readFirstRows(dir: File, n: Int): List[Map[String, String]] = {
    val conf = new Configuration()
    val files = dir.listFiles()
    if (files == null) return Nil

    val parquetFiles = files.filter(_.getName.endsWith(".parquet")).sorted
    val result = scala.collection.mutable.ListBuffer.empty[Map[String, String]]

    for (f <- parquetFiles if result.size < n) {
      val reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(new Path(f.getAbsolutePath), conf)
      )
      try {
        val schema = reader.getFooter.getFileMetaData.getSchema
        var pages = reader.readNextRowGroup()
        while (pages != null && result.size < n) {
          val columnIO = new ColumnIOFactory().getColumnIO(schema)
          val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))
          val rowCount = pages.getRowCount
          var i = 0L
          while (i < rowCount && result.size < n) {
            val group = recordReader.read()
            val row = schema.getFields.asScala.map { field =>
              field.getName -> group.getValueToString(schema.getFieldIndex(field.getName), 0)
            }.toMap
            result += row
            i += 1
          }
          pages = reader.readNextRowGroup()
        }
      } finally reader.close()
    }
    result.toList
  }
}
