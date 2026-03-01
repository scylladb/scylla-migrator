package com.scylladb.migrator.alternator

import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  DeleteBucketRequest,
  DeleteObjectRequest,
  HeadBucketRequest,
  ListObjectsV2Request,
  NoSuchBucketException,
  PutObjectRequest
}

import java.io.File
import java.net.URI

/** Shared S3 test utilities for LocalStack-backed tests. */
object S3TestUtils {

  /** Create an S3Client configured for LocalStack at localhost:4566. */
  def localStackS3Client(region: Region = Region.EU_CENTRAL_1): S3Client =
    S3Client
      .builder()
      .endpointOverride(new URI("http://localhost:4566"))
      .region(region)
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))
      )
      .forcePathStyle(true)
      .build()

  /** Delete all objects in a bucket and then delete the bucket itself. No-op if bucket doesn't exist. */
  def deleteBucket(s3Client: S3Client, bucketName: String): Unit =
    try {
      s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
      s3Client
        .listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucketName).build())
        .stream()
        .forEach { response =>
          response.contents().stream().forEach { s3Object =>
            s3Client.deleteObject(
              DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build()
            )
          }
        }
      s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())
    } catch {
      case _: NoSuchBucketException => ()
    }

  /** Recursively upload a local directory to an S3 bucket under the given prefix. */
  def uploadDirToS3(s3Client: S3Client, bucketName: String, localDir: File, prefix: String): Unit =
    if (localDir.isDirectory) {
      val files = localDir.listFiles()
      if (files != null)
        files
          .filterNot(_.getName.startsWith("."))
          .foreach { f =>
            val key = s"${prefix}/${f.getName}"
            if (f.isDirectory) uploadDirToS3(s3Client, bucketName, f, key)
            else
              s3Client.putObject(
                PutObjectRequest.builder().bucket(bucketName).key(key).build(),
                f.toPath
              )
          }
    }
}
