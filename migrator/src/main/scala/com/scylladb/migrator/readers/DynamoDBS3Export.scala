package com.scylladb.migrator.readers

import com.scylladb.migrator.AwsUtils
import com.scylladb.migrator.config.SourceSettings
import com.scylladb.migrator.config.SourceSettings.DynamoDBS3Export.{ AttributeType, KeyType }
import io.circe.{ Decoder, DecodingFailure }
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.decode
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  KeySchemaElement,
  ProvisionedThroughputDescription,
  ScalarAttributeType,
  TableDescription,
  KeyType => DdbKeyType
}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.util.Base64
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.jdk.CollectionConverters._

object DynamoDBS3Export {

  val log = LogManager.getLogger("com.scylladb.migrator.readers.DynamoDBS3Export")

  def buildS3Client(source: SourceSettings.DynamoDBS3Export): S3Client = {
    val s3ClientBuilder =
      AwsUtils
        .configureClientBuilder(
          S3Client.builder(),
          source.endpoint,
          source.region,
          source.finalCredentials.map(_.toProvider)
        )
    for (usePathStyleAccess <- source.usePathStyleAccess) {
      s3ClientBuilder.forcePathStyle(usePathStyleAccess)
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
          .fromInputStream(
            s3Client
              .getObjectAsBytes(
                GetObjectRequest.builder().bucket(source.bucket).key(source.manifestKey).build())
              .asInputStream()
          )
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
          s3Client
            .getObjectAsBytes(
              GetObjectRequest
                .builder()
                .bucket(source.bucket)
                .key(summary.manifestFilesS3Key)
                .build())
            .asInputStream()
        )
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
              .fromInputStream(
                new GZIPInputStream(
                  s3Client
                    .getObjectAsBytes(
                      GetObjectRequest
                        .builder()
                        .bucket(source.bucket)
                        .key(manifestFile.dataFileS3Key)
                        .build())
                    .asInputStream()
                ))
              .getLines()
          }
        }
        .map(decode(_)(itemDecoder)
          .fold(error => sys.error(s"Error while decoding item data: ${error}"), identity))

    // Make up a table description although we don’t have an actual table as a source but a data export
    val tableDescription =
      TableDescription
        .builder()
        .keySchema(
          source.tableDescription.keySchema.map(
            key =>
              KeySchemaElement
                .builder()
                .attributeName(key.name)
                .keyType(key.`type` match {
                  case KeyType.Hash  => DdbKeyType.HASH
                  case KeyType.Range => DdbKeyType.RANGE
                })
                .build()
          ): _*
        )
        .attributeDefinitions(
          source.tableDescription.attributeDefinitions.map(
            attr =>
              AttributeDefinition
                .builder()
                .attributeName(attr.name)
                .attributeType(attr.`type` match {
                  case AttributeType.S => ScalarAttributeType.S
                  case AttributeType.N => ScalarAttributeType.N
                  case AttributeType.B => ScalarAttributeType.B
                })
                .build()
          ): _*
        )
        .provisionedThroughput(ProvisionedThroughputDescription.builder().build())
        .build()

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
    implicit val binaryDataDecoder: Decoder[SdkBytes] =
      Decoder.decodeString.map(str => SdkBytes.fromByteArray(Base64.getDecoder.decode(str)))

    lazy val attributeValueDecoder: Decoder[AttributeValue] = Decoder.decodeJsonObject.map {
      jsonObject =>
        import com.scylladb.migrator.AttributeValueUtils._

        (jsonObject.toMap.head match {
          case ("S", value)    => value.as[String].map(AttributeValue.fromS)
          case ("N", value)    => value.as[String].map(AttributeValue.fromN)
          case ("BOOL", value) => value.as[Boolean].map(AttributeValue.fromBool(_))
          case ("L", value) =>
            value
              .as(Decoder.decodeList(attributeValueDecoder))
              .map(l => AttributeValue.fromL(l.asJava))
          case ("NULL", _)  => Right(AttributeValue.fromNul(true))
          case ("B", value) => value.as[SdkBytes].map(AttributeValue.fromB)
          case ("M", value) =>
            value.as(dataDecoder).map(m => AttributeValue.fromM(m.asJava))
          case ("SS", value) =>
            value
              .as[List[String]]
              .map(ss => AttributeValue.fromSs(ss.asJava))
          case ("NS", value) =>
            value
              .as[List[String]]
              .map(ns => AttributeValue.fromNs(ns.asJava))
          case ("BS", value) =>
            value
              .as[List[SdkBytes]]
              .map(bs => AttributeValue.fromBs(bs.asJava))
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
