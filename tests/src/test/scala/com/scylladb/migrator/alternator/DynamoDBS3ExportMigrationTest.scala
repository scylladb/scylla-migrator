package com.scylladb.migrator.alternator

import com.scylladb.migrator.SparkUtils.successfullyPerformMigration
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, AttributeValue, DeleteTableRequest, KeySchemaElement, KeyType, ScalarAttributeType}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, HeadBucketRequest, ListObjectsV2Request, NoSuchBucketException, PutObjectRequest}

import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.function.Consumer
import scala.jdk.CollectionConverters._

class DynamoDBS3ExportMigrationTest extends MigratorSuite {

  val s3Client: S3Client =
    S3Client
      .builder()
      .endpointOverride(new URI("http://127.0.0.1:4566"))
      .region(Region.EU_WEST_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
      .build()

  withResources("test-bucket", "BasicTest").test("Export data from DynamoDB to S3, and import from S3 to Alternator") { case (bucketName, tableName) =>
    // Ideally, we would like to perform an export from the DynamoDB source database, but
    // it seems impossible to tell DynamoDB to export to our local S3 instance. Instead,
    // we copy the files that resulted from a manual DynamoDB export into our local S3
    // instance.
    val bucketPath = Paths.get("src/test/s3/test-bucket")
    Files
      .walk(bucketPath)
      .forEach(new Consumer[Path] {
        def accept(path: Path): Unit = {
          if (Files.isRegularFile(path)) {
            val key = bucketPath.relativize(path).toString
            s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(), path)
          }
        }
      })

    // Perform the migration
    successfullyPerformMigration("dynamodb-s3-export-to-alternator-basic.yaml")

    // Check that the schema has been correctly defined on the target table
    checkSchemaWasMigrated(
      tableName,
      Seq(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build()).asJava,
      Seq(AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build()).asJava,
      Nil.asJava,
      Nil.asJava
    )

    // Check that the items have been migrated to the target table
    val item1Key = Map("id" -> AttributeValue.fromS("foo"))
    val item1Data = item1Key ++ Map("bar" -> AttributeValue.fromN("42"))
    checkItemWasMigrated(tableName, item1Key, item1Data)

    val item2Key = Map("id" -> AttributeValue.fromS("bar"))
    val item2Data = item2Key ++ Map("baz" -> AttributeValue.fromN("0"))
    checkItemWasMigrated(tableName, item2Key, item2Data)

    // Check that all the data types have been correctly decoded during the import
    val item3Key = Map("id" -> AttributeValue.fromS("K3TXQk84Si"))
    val item3Data = item3Key ++ Map(
      "a" -> AttributeValue.fromBool(false),
      "b" -> AttributeValue.fromNul(true),
      "c" -> AttributeValue.fromSs(List("8gfX8dh0xa", "fRjHZJ2Ce9").asJava),
      "d" -> AttributeValue.fromNs(List("3", "2").asJava),
      "e" -> AttributeValue.fromL(List(AttributeValue.fromN("1"), AttributeValue.fromS("Dsb87qc6Es")).asJava),
      "f" -> AttributeValue.fromM(
        Map(
          "g" -> AttributeValue.fromN("9"),
          "h" -> AttributeValue.fromNul(true),
          "i" -> AttributeValue.fromBool(true)
        ).asJava
      ),
      "j" -> AttributeValue.fromB(SdkBytes.fromUtf8String("hello")),
      "k" -> AttributeValue.fromBs(
        List(
          SdkBytes.fromUtf8String("is fast"),
          SdkBytes.fromUtf8String("scylladb")
        ).asJava
      )
    )
    checkItemWasMigrated(tableName, item3Key, item3Data)
  }

  // Make sure to properly set up and clean up the target database and the S3 instance
  def withResources(bucketName: String, tableName: String): FunFixture[(String, String)] = FunFixture(
    setup = _ => {
      deleteTableIfExists(targetAlternator, tableName)
      deleteBucket(bucketName)
      s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())
      (bucketName, tableName)
    },
    teardown = _ => {
      targetAlternator.deleteTable(DeleteTableRequest.builder().tableName(tableName).build())
      deleteBucket(bucketName)
    }
  )

  def deleteBucket(bucketName: String): Unit = {
    val bucketExists =
      try {
        s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
        true
      } catch {
        case _: NoSuchBucketException => false
      }
    if (bucketExists) {
      s3Client.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucketName).build())
        .stream()
        .forEach { response =>
          response.contents().stream().forEach { s3Object =>
            s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key).build())
          }
        }

      s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
    }
  }

}
