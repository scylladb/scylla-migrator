package com.scylladb.migrator.readers

import com.amazonaws.services.dynamodbv2.model.{
  AttributeDefinition,
  KeySchemaElement,
  ProvisionedThroughputDescription,
  TableDescription
}
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3ClientBuilder }
import com.scylladb.migrator.AwsUtils
import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.config.SourceSettings.DynamoDBS3Export.{ AttributeType, KeyType }
import io.circe.{ Decoder, DecodingFailure }
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.decode
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

import java.nio.ByteBuffer
import java.util.Base64
import java.util.zip.GZIPInputStream
import scala.collection.immutable.ArraySeq
import scala.io.Source

object DynamoDBS3Export {

  val log = LogManager.getLogger("com.scylladb.migrator.readers.DynamoDBS3Export")

  def buildS3Client(source: SourceSettings.DynamoDBS3Export): AmazonS3 = {
    val s3ClientBuilder =
      AwsUtils
        .configureClientBuilder(
          AmazonS3ClientBuilder.standard(),
          source.endpoint,
          source.region,
          source.finalCredentials.map(_.toProvider)
        )
    for (usePathStyleAccess <- source.usePathStyleAccess) {
      s3ClientBuilder.withPathStyleAccessEnabled(usePathStyleAccess)
    }
    s3ClientBuilder.build()
  }

  /**
    * Read the DynamoDB S3 Export data as described here:
    * [[https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html]]
    */
  def readRDD(source: SourceSettings.DynamoDBS3Export)(
    implicit spark: SparkContext): (RDD[Map[String, AttributeValue]], TableDescription) = {

    val s3Client = buildS3Client(source)

    // The entry point is a single file containing a manifest summary
    val summary =
      decode[ManifestSummary](
        Source
          .fromInputStream(s3Client.getObject(source.bucket, source.manifestKey).getObjectContent)
          .mkString
      ).fold(error => sys.error(s"Unable to read the export manifest summary: ${error}"), identity)

    for (exportType <- summary.exportType) {
      if (exportType != fullExportType) {
        sys.error(s"Unsupported export type: ${summary.exportType.get}")
      }
    }

    if (summary.outputFormat != dynamoDBJsonFormat) {
      sys.error(s"Unsupported export format: ${summary.outputFormat}")
    }

    log.info(s"Found DynamoDB S3 export containing ${summary.itemCount} items")

    // The manifest summary links to the “manifest files”, which lists all the JSON files containing
    // the actual data. The “manifest files” is a text file where each line contains a JSON object
    // describing a single file containing the actual data.
    val manifestFiles =
      Source
        .fromInputStream(
          s3Client.getObject(source.bucket, summary.manifestFilesS3Key).getObjectContent)
        .getLines()
        .toSeq // Load all the manifest files to fail early in case of issue
        .map { manifestFileJson =>
          // Parse each line as JSON and decode the relevant information
          decode[ManifestFile](manifestFileJson).fold(
            error =>
              sys.error(
                s"Unable to parse the manifest file ${summary.manifestFilesS3Key}: ${error}"),
            identity
          )
        }

    // Load as efficiently as possible all the files that contain the actual data
    // Based on https://medium.com/@fmonteiro.alex/improving-apache-spark-processing-performance-when-reading-small-files-dd240ea4be6d
    val rdd =
      spark
        .parallelize(manifestFiles)
        .mapPartitions { manifestFilesPartition =>
          val s3Client = buildS3Client(source)
          manifestFilesPartition.flatMap { manifestFile =>
            Source
              .fromInputStream(new GZIPInputStream(
                s3Client.getObject(source.bucket, manifestFile.dataFileS3Key).getObjectContent))
              .getLines()
          }
        }
        .map(decode(_)(itemDecoder)
          .fold(error => sys.error(s"Error while decoding item data: ${error}"), identity))

    // Make up a table description although we don’t have an actual table as a source but a data export
    val tableDescription =
      new TableDescription()
        .withKeySchema(source.tableDescription.keySchema.map(key =>
          new KeySchemaElement(key.name, key.`type` match {
            case KeyType.Hash  => "HASH"
            case KeyType.Range => "RANGE"
          })): _*)
        .withAttributeDefinitions(source.tableDescription.attributeDefinitions.map(attr =>
          new AttributeDefinition(attr.name, attr.`type` match {
            case AttributeType.S => "S"
            case AttributeType.N => "N"
            case AttributeType.B => "B"
          })): _*)
        .withProvisionedThroughput(new ProvisionedThroughputDescription())

    (rdd, tableDescription)
  }

  case class ManifestSummary(manifestFilesS3Key: String,
                             itemCount: Int,
                             exportType: Option[String],
                             outputFormat: String)
  object ManifestSummary {
    implicit val jsonDecoder: Decoder[ManifestSummary] = deriveDecoder[ManifestSummary]
  }

  case class ManifestFile(dataFileS3Key: String)
  object ManifestFile {
    implicit val jsonDecoder: Decoder[ManifestFile] = deriveDecoder[ManifestFile]
  }

  // Decode the DynamoDB JSON format as shown here:
  // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html
  val itemDecoder: Decoder[Map[String, AttributeValue]] = {
    implicit val binaryDataDecoder: Decoder[ByteBuffer] =
      Decoder.decodeString.map(str => ByteBuffer.wrap(Base64.getDecoder.decode(str)))

    lazy val attributeValueDecoder: Decoder[AttributeValue] = Decoder.decodeJsonObject.map {
      jsonObject =>
        import com.scylladb.migrator.AttributeValueUtils._

        (jsonObject.toMap.head match {
          case ("S", value)    => value.as[String].map(stringValue)
          case ("N", value)    => value.as[String].map(numericalValue)
          case ("BOOL", value) => value.as[Boolean].map(boolValue)
          case ("L", value) =>
            value
              .as(Decoder.decodeArray(attributeValueDecoder, Array.toFactory(Array)))
              .map(l => listValue(ArraySeq.unsafeWrapArray(l): _*))
          case ("NULL", _)  => Right(nullValue)
          case ("B", value) => value.as[ByteBuffer].map(binaryValue)
          case ("M", value) =>
            value.as(dataDecoder).map(mapValue)
          case ("SS", value) =>
            value
              .as[Array[String]]
              .map(ss => stringValues(ArraySeq.unsafeWrapArray(ss): _*))
          case ("NS", value) =>
            value
              .as[Array[String]]
              .map(ns => numericalValues(ArraySeq.unsafeWrapArray(ns): _*))
          case ("BS", value) =>
            value
              .as[Array[ByteBuffer]]
              .map(byteBuffers => binaryValues(ArraySeq.unsafeWrapArray(byteBuffers): _*))
          case (tpe, _) => Left(DecodingFailure(s"Unknown item type: ${tpe}", Nil))
        }).fold(error => sys.error(s"Unable to decode AttributeValue: ${error}"), identity)
    }

    lazy val dataDecoder: Decoder[Map[String, AttributeValue]] =
      Decoder.decodeJsonObject.emap { jsonObject =>
        jsonObject.toMap.foldLeft[Either[String, Map[String, AttributeValue]]](Right(Map.empty)) {
          case (acc, (key, value)) =>
            for {
              previousAttr <- acc
              decodedValue <- attributeValueDecoder.decodeJson(value).left.map(_.toString())
            } yield previousAttr + (key -> decodedValue)
        }
      }

    Decoder.instance { cursor =>
      for {
        item <- cursor.downField("Item").as(dataDecoder)
      } yield item
    }
  }

  val fullExportType = "FULL_EXPORT"
  val dynamoDBJsonFormat = "DYNAMODB_JSON"
}
