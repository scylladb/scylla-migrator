package com.scylladb.migrator.alternator

import java.io.File

object S3ExportE2EBenchmarkUtils {
  val s3ExportHostDir = new File("tests/docker/parquet/bench_s3export")

  def deleteS3ExportDir(): Unit =
    if (s3ExportHostDir.exists()) {
      def deleteRecursive(f: File): Unit = {
        if (f.isDirectory) f.listFiles().foreach(deleteRecursive)
        f.delete()
      }
      deleteRecursive(s3ExportHostDir)
    }

  def hasExportFiles: Boolean =
    s3ExportHostDir.exists() &&
      new File(s3ExportHostDir, "manifest-summary.json").exists() &&
      new File(s3ExportHostDir, "data").exists()
}
