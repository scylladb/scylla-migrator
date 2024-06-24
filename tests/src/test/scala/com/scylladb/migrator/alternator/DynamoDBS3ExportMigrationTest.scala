package com.scylladb.migrator.alternator

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, AttributeValue, KeySchemaElement}
import com.amazonaws.services.s3.model.{AmazonS3Exception, HeadBucketRequest, ObjectListing}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.scylladb.migrator.SparkUtils.successfullyPerformMigration

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}
import java.util.function.Consumer
import scala.jdk.CollectionConverters._

class DynamoDBS3ExportMigrationTest extends MigratorSuite {

  val s3Client: AmazonS3 =
    AmazonS3ClientBuilder
      .standard()
      .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:4566", "us-east-1"))
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("dummy", "dummy")))
      .build()

  withResources("test-bucket", "BasicTest").test("Export data from DynamoDB to S3, and import from S3 to Alternator") { case (bucketName, tableName) =>
    // Ideally, we would like to perform an export from the DynamoDB source database, but
    // it seems impossible to tell DynamoDB to export to our local S3 instance. Instead,
    // we copy the files that resulted from a manual DynamoDB export into our local S3
    // instance.
    val bucketPath = Paths.get("tests/src/test/s3/test-bucket")
    Files
      .walk(bucketPath)
      .forEach(new Consumer[Path] {
        def accept(path: Path): Unit = {
          if (Files.isRegularFile(path)) {
            val key = bucketPath.relativize(path).toString
            s3Client.putObject(bucketName, key, path.toFile)
          }
        }
      })

    // Perform the migration
    successfullyPerformMigration("dynamodb-s3-export-to-alternator-basic.yaml")

    // Check that the schema has been correctly defined on the target table
    checkSchemaWasMigrated(
      tableName,
      Seq(new KeySchemaElement("id", "HASH")).asJava,
      Seq(new AttributeDefinition("id", "S")).asJava
    )

    // Check that the items have been migrated to the target table
    val item1Key = Map("id" -> new AttributeValue().withS("foo"))
    val item1Data = item1Key ++ Map("bar" -> new AttributeValue().withN("42"))
    checkItemWasMigrated(tableName, item1Key, item1Data)

    val item2Key = Map("id" -> new AttributeValue().withS("bar"))
    val item2Data = item2Key ++ Map("baz" -> new AttributeValue().withN("0"))
    checkItemWasMigrated(tableName, item2Key, item2Data)

    // Check that all the data types have been correctly decoded during the import
    val item3Key = Map("id" -> new AttributeValue().withS("K3TXQk84Si"))
    val item3Data = item3Key ++ Map(
      "a" -> new AttributeValue().withBOOL(false),
      "b" -> new AttributeValue().withNULL(true),
      "c" -> new AttributeValue().withSS("8gfX8dh0xa", "fRjHZJ2Ce9"),
      "d" -> new AttributeValue().withNS("3", "2"),
      "e" -> new AttributeValue().withL(new AttributeValue().withN("1"), new AttributeValue().withS("Dsb87qc6Es")),
      "f" -> new AttributeValue().withM(
        Map(
          "g" -> new AttributeValue().withN("9"),
          "h" -> new AttributeValue().withNULL(true),
          "i" -> new AttributeValue().withBOOL(true)
        ).asJava
      ),
      "j" -> new AttributeValue().withB(ByteBuffer.wrap("hello".getBytes)),
      "k" -> new AttributeValue().withBS(
        ByteBuffer.wrap("is fast".getBytes),
        ByteBuffer.wrap("scylladb".getBytes)
      )
    )
    checkItemWasMigrated(tableName, item3Key, item3Data)
  }

  // Make sure to properly set up and clean up the target database and the S3 instance
  def withResources(bucketName: String, tableName: String): FunFixture[(String, String)] = FunFixture(
    setup = _ => {
      deleteTableIfExists(targetAlternator, tableName)
      deleteBucket(bucketName)
      s3Client.createBucket(bucketName)
      (bucketName, tableName)
    },
    teardown = _ => {
      targetAlternator.deleteTable(tableName)
      deleteBucket(bucketName)
    }
  )

  def deleteBucket(bucketName: String): Unit = {
    val bucketExists =
      try {
        s3Client.headBucket(new HeadBucketRequest(bucketName))
        true
      } catch {
        case _: AmazonS3Exception => false
      }
    if (bucketExists) {
      // Remove the existing objects
      def deleteObjects(listing: ObjectListing): Unit = {
        for (summary <- listing.getObjectSummaries.asScala) {
          s3Client.deleteObject(bucketName, summary.getKey)
        }
        if (listing.isTruncated) {
          deleteObjects(s3Client.listNextBatchOfObjects(listing))
        }
      }

      deleteObjects(s3Client.listObjects(bucketName))

      s3Client.deleteBucket(bucketName)
    }
  }

}
