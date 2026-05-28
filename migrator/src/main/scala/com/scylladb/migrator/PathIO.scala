package com.scylladb.migrator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path => HadoopPath, UnsupportedFileSystemException }
import org.apache.logging.log4j.LogManager

import java.io.{ ByteArrayOutputStream, IOException }
import java.nio.charset.StandardCharsets
import java.nio.file.{
  AccessDeniedException,
  AtomicMoveNotSupportedException,
  Files,
  Path => NioPath,
  Paths,
  StandardCopyOption
}
import java.util.Locale
import scala.jdk.CollectionConverters._
import scala.util.Using

private[migrator] trait PathIO {
  def normalize(path: String): String
  def exists(path: String): Boolean
  def createDirectories(path: String): Unit
  def listFileNames(path: String): Seq[String]
  def readUtf8(path: String): String
  def writeUtf8Atomically(path: String, payload: Array[Byte]): Unit
}

private[migrator] object PathIO {
  private val log = LogManager.getLogger(getClass)
  private val UriScheme = """^([A-Za-z][A-Za-z0-9+.-]*):/{2}.*""".r

  def forPath(path: String, hadoopConfiguration: Option[Configuration]): PathIO =
    scheme(path) match {
      case Some("file") | None => LocalPathIO
      case Some(_) =>
        new HadoopPathIO(
          path,
          hadoopConfiguration.map(new Configuration(_)).getOrElse(new Configuration())
        )
    }

  private def scheme(path: String): Option[String] =
    path match {
      case UriScheme(rawScheme) => Some(rawScheme.toLowerCase(Locale.ROOT))
      case _ if path.regionMatches(true, 0, "file:", 0, "file:".length) => Some("file")
      case _                                                            => None
    }

  private object LocalPathIO extends PathIO {
    private def toPath(path: String): NioPath =
      scheme(path) match {
        case Some("file") => Paths.get(new HadoopPath(path).toUri).normalize()
        case _            => Paths.get(path).normalize()
      }

    override def normalize(path: String): String = toPath(path).toString

    override def exists(path: String): Boolean = Files.exists(toPath(path))

    override def createDirectories(path: String): Unit =
      Files.createDirectories(toPath(path))

    override def listFileNames(path: String): Seq[String] =
      Using.resource(Files.list(toPath(path))) { stream =>
        stream.iterator().asScala.map(_.getFileName.toString).toSeq
      }

    override def readUtf8(path: String): String =
      new String(Files.readAllBytes(toPath(path)), StandardCharsets.UTF_8)

    override def writeUtf8Atomically(path: String, payload: Array[Byte]): Unit = {
      val finalPath = toPath(path)
      val tempPath = Paths.get(finalPath.toString + ".tmp").normalize()

      var moved = false
      try {
        Files.write(tempPath, payload)
        atomicReplace(tempPath, finalPath)
        moved = true
      } finally
        if (!moved) deleteTemp(tempPath)
    }

    private def atomicReplace(source: NioPath, target: NioPath): Unit =
      try
        Files.move(
          source,
          target,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING
        )
      catch {
        case _: AtomicMoveNotSupportedException =>
          // Fallback for filesystems that do not support atomic rename (e.g. certain object-store
          // mounts). Semantics degrade to "replace" but all other guarantees are preserved.
          log.warn(
            s"Atomic rename not supported on the filesystem backing ${target}; " +
              s"falling back to non-atomic replace."
          )
          Files.move(source, target, StandardCopyOption.REPLACE_EXISTING)
        case e: AccessDeniedException =>
          // On Windows, ATOMIC_MOVE can throw AccessDeniedException if a reader holds the target
          // file open. Fall back to a non-atomic replace rather than failing the dump.
          log.warn(
            s"Atomic rename denied on ${target} (likely a concurrent reader on Windows); " +
              s"falling back to non-atomic replace: ${e.getMessage}"
          )
          Files.move(source, target, StandardCopyOption.REPLACE_EXISTING)
      }

    private def deleteTemp(tempPath: NioPath): Unit =
      try Files.deleteIfExists(tempPath)
      catch {
        case cleanupErr: Throwable =>
          log.warn(s"Failed to clean up temp savepoint ${tempPath}: ${cleanupErr.getMessage}")
      }
  }

  private class HadoopPathIO(initialPath: String, hadoopConfiguration: Configuration)
      extends PathIO {
    private val fs = fileSystemFor(initialPath)

    override def normalize(path: String): String = new HadoopPath(path).toString

    override def exists(path: String): Boolean = fs.exists(toPath(path))

    override def createDirectories(path: String): Unit = {
      val directory = toPath(path)
      if (!fs.mkdirs(directory))
        throw new IOException(s"Failed to create directory ${directory}")
    }

    override def listFileNames(path: String): Seq[String] =
      fs.listStatus(toPath(path)).iterator.map(_.getPath.getName).toSeq

    override def readUtf8(path: String): String =
      Using.resource(fs.open(toPath(path))) { in =>
        val out = new ByteArrayOutputStream()
        val buffer = new Array[Byte](8192)
        var read = in.read(buffer)
        while (read != -1) {
          out.write(buffer, 0, read)
          read = in.read(buffer)
        }
        out.toString(StandardCharsets.UTF_8.name())
      }

    override def writeUtf8Atomically(path: String, payload: Array[Byte]): Unit = {
      val finalPath = toPath(path)
      val tempPath = toPath(finalPath.toString + ".tmp")

      var moved = false
      try {
        Using.resource(fs.create(tempPath, true))(_.write(payload))
        moved = fs.rename(tempPath, finalPath)
        if (!moved) throw new IOException(s"Failed to rename ${tempPath} to ${finalPath}")
      } finally
        if (!moved) deleteTemp(tempPath)
    }

    private def toPath(path: String): HadoopPath = new HadoopPath(path)

    private def fileSystemFor(path: String): FileSystem =
      try toPath(path).getFileSystem(hadoopConfiguration)
      catch {
        case e: UnsupportedFileSystemException =>
          val pathScheme = Option(toPath(path).toUri.getScheme).getOrElse("<none>")
          throw new IllegalArgumentException(
            s"Path ${path} uses Hadoop filesystem scheme '${pathScheme}', but no implementation " +
              s"is configured. ${connectorGuidance(pathScheme)}",
            e
          )
      }

    private def connectorGuidance(pathScheme: String): String =
      pathScheme.toLowerCase(Locale.ROOT) match {
        case "gs" =>
          "For gs:// paths, include the Hadoop GCS connector in the Spark runtime and configure " +
            "its credentials via Spark/Hadoop configuration."
        case "s3" | "s3a" | "s3n" =>
          "For s3a:// paths, include the Hadoop AWS/S3A connector in the Spark runtime and " +
            "configure its credentials via Spark/Hadoop configuration."
        case other =>
          s"Include a Hadoop filesystem connector for '${other}' paths in the Spark runtime and " +
            "configure it via Spark/Hadoop configuration."
      }

    private def deleteTemp(tempPath: HadoopPath): Unit =
      try fs.delete(tempPath, false)
      catch {
        case cleanupErr: Throwable =>
          log.warn(s"Failed to clean up temp savepoint ${tempPath}: ${cleanupErr.getMessage}")
      }
  }
}
