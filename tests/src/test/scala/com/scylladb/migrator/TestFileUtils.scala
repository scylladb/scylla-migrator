package com.scylladb.migrator

import org.apache.log4j.LogManager

import java.io.File

object TestFileUtils {
  private val log = LogManager.getLogger("com.scylladb.migrator.TestFileUtils")

  /** Recursively delete a file or directory. Safe against concurrent deletion. */
  def deleteRecursive(f: File): Unit = {
    if (f.isDirectory) {
      val children = f.listFiles()
      if (children != null) children.foreach(deleteRecursive)
    }
    if (f.exists() && !f.delete())
      log.warn(s"Failed to delete ${f.getAbsolutePath}")
  }
}
