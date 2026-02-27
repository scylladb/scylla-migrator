package com.scylladb.migrator.writers

import com.scylladb.migrator.config.TargetSettings
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.{ AttributeValue, TableDescription }

import java.io.{ BufferedOutputStream, OutputStream }
import java.util.Base64
import java.util.zip.GZIPOutputStream
import scala.jdk.CollectionConverters._

object DynamoDBS3Export {
  val log = LogManager.getLogger("com.scylladb.migrator.writers.DynamoDBS3Export")

  /** Write a DynamoDB RDD as a DynamoDB S3 Export-compatible directory.
    *
    * Produces:
    *   - `data/` directory with gzipped JSON line files (one per partition)
    *   - `manifest-files.json` listing the data files
    *   - `manifest-summary.json` with item count and metadata
    */
  def writeRDD(
    target: TargetSettings.DynamoDBS3Export,
    rdd: RDD[(Text, DynamoDBItemWritable)],
    sourceTableDesc: TableDescription
  )(implicit spark: SparkSession): Unit = {
    val path = target.path
    log.info(s"Writing DynamoDB S3 Export to ${path}")

    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Write gzipped JSON data files, one per partition
    val dataPath = s"${path}/data"
    val itemCounts = rdd
      .mapPartitionsWithIndex { case (idx, items) =>
        val fileName = f"${idx}%05d.json.gz"
        val filePath = new org.apache.hadoop.fs.Path(s"${dataPath}/${fileName}")
        val fs = filePath.getFileSystem(new org.apache.hadoop.conf.Configuration(hadoopConf))
        var count = 0L
        val out: OutputStream = new GZIPOutputStream(new BufferedOutputStream(fs.create(filePath)))
        try
          items.foreach { case (_, itemWritable) =>
            val json = encodeItem(itemWritable.getItem.asScala.toMap)
            out.write(json.getBytes("UTF-8"))
            out.write('\n')
            count += 1
          }
        finally
          out.close()
        Iterator((idx, count))
      }
      .collect()

    val totalItems = itemCounts.map(_._2).sum

    // Write manifest-files.json
    val manifestFilesPath = new org.apache.hadoop.fs.Path(s"${path}/manifest-files.json")
    val fs = manifestFilesPath.getFileSystem(hadoopConf)
    val manifestOut = fs.create(manifestFilesPath)
    try
      itemCounts.foreach { case (idx, count) =>
        val fileName = f"${idx}%05d.json.gz"
        val dataFileKey = s"data/${fileName}"
        manifestOut.write(
          s"""{"itemCount":${count},"dataFileS3Key":"${dataFileKey}"}""".getBytes("UTF-8")
        )
        manifestOut.write('\n')
      }
    finally
      manifestOut.close()

    // Write manifest-summary.json
    val summaryPath = new org.apache.hadoop.fs.Path(s"${path}/manifest-summary.json")
    val summaryOut = fs.create(summaryPath)
    try
      summaryOut.write(
        s"""{"version":"2020-06-30","manifestFilesS3Key":"manifest-files.json","itemCount":${totalItems},"outputFormat":"DYNAMODB_JSON"}"""
          .getBytes("UTF-8")
      )
    finally
      summaryOut.close()

    log.info(
      s"Successfully wrote DynamoDB S3 Export: ${totalItems} items in ${itemCounts.length} files"
    )
  }

  /** Encode a DynamoDB item as a DynamoDB JSON line: `{"Item":{...}}` */
  def encodeItem(item: Map[String, AttributeValue]): String = {
    val fields = item.map { case (k, v) => s""""${escapeJson(k)}":${encodeAttributeValue(v)}""" }
    s"""{"Item":{${fields.mkString(",")}}}"""
  }

  /** Encode an AttributeValue to its DynamoDB JSON representation */
  def encodeAttributeValue(av: AttributeValue): String =
    if (av.s() != null) s"""{"S":"${escapeJson(av.s())}"}"""
    else if (av.n() != null) s"""{"N":"${escapeJson(av.n())}"}"""
    else if (av.b() != null) {
      val b64 = Base64.getEncoder.encodeToString(av.b().asByteArray())
      s"""{"B":"${b64}"}"""
    } else if (av.hasSs) {
      val elems = av.ss().asScala.map(s => s""""${escapeJson(s)}"""").mkString(",")
      s"""{"SS":[${elems}]}"""
    } else if (av.hasNs) {
      val elems = av.ns().asScala.map(n => s""""${escapeJson(n)}"""").mkString(",")
      s"""{"NS":[${elems}]}"""
    } else if (av.hasBs) {
      val elems = av
        .bs()
        .asScala
        .map(b => s""""${Base64.getEncoder.encodeToString(b.asByteArray())}"""")
        .mkString(",")
      s"""{"BS":[${elems}]}"""
    } else if (av.hasL) {
      val elems = av.l().asScala.map(encodeAttributeValue).mkString(",")
      s"""{"L":[${elems}]}"""
    } else if (av.hasM) {
      val fields = av.m().asScala.map { case (k, v) =>
        s""""${escapeJson(k)}":${encodeAttributeValue(v)}"""
      }
      s"""{"M":{${fields.mkString(",")}}}"""
    } else if (av.nul() != null && av.nul()) s"""{"NULL":true}"""
    else if (av.bool() != null) s"""{"BOOL":${av.bool()}}"""
    else sys.error(s"Unable to encode AttributeValue: ${av}")

  private def escapeJson(s: String): String =
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
}
