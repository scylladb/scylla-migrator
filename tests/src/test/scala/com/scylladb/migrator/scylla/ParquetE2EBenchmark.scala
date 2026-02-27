package com.scylladb.migrator.scylla

import java.io.File

object ParquetE2EBenchmarkUtils {
  val parquetHostDir = new File("tests/docker/parquet/bench_e2e")

  def deleteParquetDir(): Unit =
    if (parquetHostDir.exists()) {
      def deleteRecursive(f: File): Unit = {
        if (f.isDirectory) f.listFiles().foreach(deleteRecursive)
        f.delete()
      }
      deleteRecursive(parquetHostDir)
    }

  def countParquetFiles(): Int =
    if (parquetHostDir.exists())
      parquetHostDir.listFiles().count(_.getName.endsWith(".parquet"))
    else 0
}
