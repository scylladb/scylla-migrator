package com.scylladb.migrator.writers

import com.scylladb.migrator.config.TargetSettings
import io.circe.{ Json, JsonObject }
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.io.Text
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
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
    *
    * The `path` can be any Hadoop-compatible filesystem path. For local testing, a local path is
    * used. For writing directly to S3, use an `s3a://` URI and ensure the appropriate Hadoop S3
    * configuration is set in the Spark context.
    */
  def writeRDD(
    target: TargetSettings.DynamoDBS3Export,
    rdd: RDD[(Text, DynamoDBItemWritable)],
    tableDescription: Option[TableDescription] = None
  )(implicit spark: SparkSession): Unit = {
    val path = target.path
    log.info(s"Writing DynamoDB S3 Export to ${path}")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val outputPath = new org.apache.hadoop.fs.Path(path)
    val fs = outputPath.getFileSystem(hadoopConf)

    // Best-effort check; not atomic on distributed filesystems (TOCTOU).
    if (fs.exists(outputPath))
      throw new IllegalStateException(
        s"Output path already exists: ${path}. Delete it or choose a different path."
      )

    val confBroadcast =
      spark.sparkContext.broadcast(new org.apache.spark.util.SerializableConfiguration(hadoopConf))

    // Write gzipped JSON data files, one per non-empty partition.
    // Empty partitions are skipped, so file indices may not be contiguous (e.g., 00000, 00003).
    val dataPath = s"${path}/data"
    try {
      val itemCounts =
        rdd
          .mapPartitionsWithIndex { case (idx, items) =>
            val buffered = items.buffered
            if (!buffered.hasNext) {
              Iterator.empty
            } else {
              val fileName = f"${idx}%05d.json.gz"
              val filePath = new org.apache.hadoop.fs.Path(s"${dataPath}/${fileName}")
              val partFs = filePath.getFileSystem(confBroadcast.value.value)
              var count = 0L
              val out: OutputStream =
                new GZIPOutputStream(new BufferedOutputStream(partFs.create(filePath)))
              try
                buffered.foreach { case (_, itemWritable) =>
                  val json = encodeItem(itemWritable.getItem.asScala.toMap)
                  out.write(json.getBytes("UTF-8"))
                  out.write('\n')
                  count += 1
                }
              finally
                out.close()
              Iterator((idx, count))
            }
          }
          .collect()

      val totalItems = itemCounts.map(_._2).sum

      // Write manifest-files.json
      val manifestFilesPath = new org.apache.hadoop.fs.Path(s"${path}/manifest-files.json")
      val manifestOut = fs.create(manifestFilesPath)
      try
        itemCounts.foreach { case (idx, count) =>
          val fileName = f"${idx}%05d.json.gz"
          val dataFileKey = s"data/${fileName}"
          manifestOut.write(
            Json
              .obj(
                "itemCount"     -> Json.fromLong(count),
                "dataFileS3Key" -> Json.fromString(dataFileKey)
              )
              .noSpaces
              .getBytes("UTF-8")
          )
          manifestOut.write('\n')
        }
      finally
        manifestOut.close()

      // Write manifest-summary.json
      val summaryPath = new org.apache.hadoop.fs.Path(s"${path}/manifest-summary.json")
      val summaryOut = fs.create(summaryPath)
      try {
        val baseFields = List(
          "version"            -> Json.fromString("2020-06-30"),
          "manifestFilesS3Key" -> Json.fromString("manifest-files.json"),
          "itemCount"          -> Json.fromLong(totalItems),
          "outputFormat"       -> Json.fromString("DYNAMODB_JSON")
        )
        val tableFields = tableDescription.toList.flatMap { desc =>
          val keySchema = Json.fromValues(
            desc.keySchema().asScala.map { ks =>
              Json.obj(
                "AttributeName" -> Json.fromString(ks.attributeName()),
                "KeyType"       -> Json.fromString(ks.keyType().toString)
              )
            }
          )
          val attrDefs = Json.fromValues(
            desc.attributeDefinitions().asScala.map { ad =>
              Json.obj(
                "AttributeName" -> Json.fromString(ad.attributeName()),
                "AttributeType" -> Json.fromString(ad.attributeType().toString)
              )
            }
          )
          List(
            "tableKeySchema"            -> keySchema,
            "tableAttributeDefinitions" -> attrDefs
          )
        }
        summaryOut.write(
          Json
            .fromJsonObject(JsonObject.fromIterable(baseFields ++ tableFields))
            .noSpaces
            .getBytes("UTF-8")
        )
      } finally
        summaryOut.close()

      log.info(
        s"Successfully wrote DynamoDB S3 Export: ${totalItems} items in ${itemCounts.length} files"
      )
    } catch {
      case e: Exception =>
        log.error(s"Write failed, cleaning up partial output at ${path}", e)
        try fs.delete(outputPath, true)
        catch {
          case cleanupEx: Exception =>
            log.warn(s"Failed to clean up partial output at ${path}", cleanupEx)
        }
        throw e
    } finally
      confBroadcast.destroy()
  }

  /** Encode a DynamoDB item as a DynamoDB JSON line: `{"Item":{...}}`
    *
    * Note: keys are sorted alphabetically for deterministic output. This differs from real AWS S3
    * Export output, which preserves DynamoDB item ordering. Roundtrip comparison with real exports
    * may show ordering differences.
    */
  def encodeItem(item: Map[String, AttributeValue]): String = {
    val fields = item.toSeq.sortBy(_._1).map { case (k, v) =>
      try k -> encodeAttributeValueJson(v)
      catch {
        case e: Exception =>
          throw new IllegalArgumentException(s"Failed to encode attribute '${k}': ${e.getMessage}", e)
      }
    }
    Json.obj("Item" -> Json.fromJsonObject(JsonObject.fromIterable(fields))).noSpaces
  }

  /** Encode an AttributeValue to its DynamoDB JSON representation */
  def encodeAttributeValue(av: AttributeValue): String =
    encodeAttributeValueJson(av).noSpaces

  private def encodeAttributeValueJson(av: AttributeValue): Json =
    if (av.s() != null) Json.obj("S" -> Json.fromString(av.s()))
    else if (av.n() != null) Json.obj("N" -> Json.fromString(av.n()))
    else if (av.b() != null) {
      val b64 = Base64.getEncoder.encodeToString(av.b().asByteArray())
      Json.obj("B" -> Json.fromString(b64))
    } else if (av.hasSs)
      Json.obj("SS" -> Json.fromValues(av.ss().asScala.map(Json.fromString)))
    else if (av.hasNs)
      Json.obj("NS" -> Json.fromValues(av.ns().asScala.map(Json.fromString)))
    else if (av.hasBs)
      Json.obj(
        "BS" -> Json.fromValues(
          av.bs()
            .asScala
            .map(b => Json.fromString(Base64.getEncoder.encodeToString(b.asByteArray())))
        )
      )
    else if (av.hasL)
      Json.obj("L" -> Json.fromValues(av.l().asScala.map(encodeAttributeValueJson)))
    else if (av.hasM) {
      val fields = av.m().asScala.toSeq.sortBy(_._1).map { case (k, v) => k -> encodeAttributeValueJson(v) }
      Json.obj("M" -> Json.fromJsonObject(JsonObject.fromIterable(fields)))
    } else if (av.nul() != null && av.nul()) Json.obj("NULL" -> Json.fromBoolean(true))
    else if (av.bool() != null) Json.obj("BOOL" -> Json.fromBoolean(av.bool()))
    else sys.error(s"Unable to encode AttributeValue: ${av}")
}
