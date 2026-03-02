package com.scylladb.migrator

import org.apache.log4j.LogManager

import java.io.File

object TestFileUtils {
  private val log = LogManager.getLogger("com.scylladb.migrator.TestFileUtils")

  /** Recursively delete a file or directory. Safe against concurrent deletion.
    *
    * Falls back to `docker compose exec` when Java's `File.delete()` fails,
    * e.g. for root-owned files created by Docker containers.
    */
  def deleteRecursive(f: File): Unit = {
    if (!f.exists()) return

    // Try Java delete first
    deleteRecursiveJava(f)

    // If Java delete didn't fully clean up (e.g. root-owned files from Docker),
    // use docker compose exec to delete inside the container where we have root.
    if (f.exists()) {
      val containerPath = resolveContainerPath(f)
      if (containerPath.nonEmpty) {
        log.info(s"Java delete failed for ${f.getAbsolutePath}, using docker compose exec to rm $containerPath")
        val exitCode = new ProcessBuilder(
          "docker", "compose", "-f", "docker-compose-tests.yml",
          "exec", "-T", "spark-master", "rm", "-rf", containerPath
        ).directory(findProjectRoot())
          .inheritIO()
          .start()
          .waitFor()
        if (exitCode != 0 && f.exists())
          log.warn(s"docker compose exec rm failed for $containerPath (exit code $exitCode)")
      } else {
        log.warn(s"Failed to delete ${f.getAbsolutePath}: cannot resolve container path")
      }
    }
  }

  private def deleteRecursiveJava(f: File): Unit = {
    if (f.isDirectory) {
      val children = f.listFiles()
      if (children != null) children.foreach(deleteRecursiveJava)
    }
    if (f.exists()) f.delete()
  }

  /** Map a host path under tests/docker/parquet/ to /app/parquet/ inside the container. */
  private def resolveContainerPath(f: File): String = {
    val abs = f.getAbsolutePath
    val marker = "/docker/parquet/"
    val idx = abs.indexOf(marker)
    if (idx >= 0) "/app/parquet/" + abs.substring(idx + marker.length)
    else ""
  }

  /** Find the project root directory containing docker-compose-tests.yml. */
  private def findProjectRoot(): File = {
    var dir = new File(System.getProperty("user.dir"))
    while (dir != null && !new File(dir, "docker-compose-tests.yml").exists())
      dir = dir.getParentFile
    if (dir != null) dir
    else new File(System.getProperty("user.dir"))
  }
}
