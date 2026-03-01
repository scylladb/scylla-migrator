package com.scylladb.migrator.alternator

import com.scylladb.migrator.TestFileUtils
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

import java.io.File

object DynamoDBS3ExportE2EBenchmarkUtils {
  // Host path maps to /app/parquet/bench_s3export inside the Spark container.
  // Relative to the sbt working directory (tests/).
  val s3ExportHostDir = new File("docker/parquet/bench_s3export")

  def deleteS3ExportDir(): Unit =
    if (s3ExportHostDir.exists())
      TestFileUtils.deleteRecursive(s3ExportHostDir)

  def hasExportFiles: Boolean =
    s3ExportHostDir.exists() &&
      new File(s3ExportHostDir, "manifest-summary.json").exists() &&
      new File(s3ExportHostDir, "data").exists()

  case class ManifestSummary(
    manifestFilesS3Key: String,
    itemCount: Long,
    outputFormat: String
  )
  object ManifestSummary {
    implicit val decoder: Decoder[ManifestSummary] = deriveDecoder[ManifestSummary]
  }

  case class ManifestFile(dataFileS3Key: String, itemCount: Long)
  object ManifestFile {
    implicit val decoder: Decoder[ManifestFile] = deriveDecoder[ManifestFile]
  }
}
