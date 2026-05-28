package com.scylladb.migrator

import com.scylladb.migrator.config.{
  AWSCredentials => ConfigAWSCredentials,
  MigratorConfig,
  Savepoints,
  SavepointsTarget,
  SourceSettings,
  TargetSettings
}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{
  FSDataInputStream,
  FSDataOutputStream,
  FileStatus,
  FileSystem,
  Path => HadoopPath,
  RawLocalFileSystem
}
import org.apache.hadoop.util.Progressable

import java.io.IOException
import java.net.URI
import java.nio.file.{ Files, Path }
import scala.util.Using

class GcsSavepointsTest extends munit.FunSuite {

  private def newConfig(savepointsPath: String): MigratorConfig =
    MigratorConfig(
      source = SourceSettings.Parquet(
        path        = "dummy",
        credentials = None,
        region      = None,
        endpoint    = None
      ),
      target = TargetSettings.Scylla(
        host                          = "localhost",
        port                          = 9042,
        localDC                       = None,
        credentials                   = None,
        sslOptions                    = None,
        keyspace                      = "ks",
        table                         = "t",
        connections                   = None,
        stripTrailingZerosForDecimals = false,
        writeTTLInS                   = None,
        writeWritetimestampInuS       = None,
        consistencyLevel              = "LOCAL_QUORUM"
      ),
      renames          = None,
      savepoints       = Savepoints(intervalSeconds = 3600, path = savepointsPath),
      skipTokenRanges  = None,
      skipSegments     = None,
      skipParquetFiles = None,
      validation       = None
    )

  private def hadoopConf(root: Path): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.gs.impl", classOf[LocalGsFileSystem].getName)
    conf.setBoolean("fs.gs.impl.disable.cache", true)
    conf.set(LocalGsFileSystem.RootConfigKey, root.toString)
    conf
  }

  private def gcsHadoopConf(root: Path): Configuration = {
    val conf = hadoopConf(root)
    conf.set(LocalGsFileSystem.ExpectedGcsAuthTypeConfigKey, "SERVICE_ACCOUNT_JSON_KEYFILE")
    conf.set(LocalGsFileSystem.ExpectedGcsJsonKeyfileConfigKey, "/etc/gcp/key.json")
    conf.set(LocalGsFileSystem.ExpectedGcsProjectIdConfigKey, "scylla-migrator-test")
    conf
  }

  private def s3HadoopConf(root: Path): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.s3a.impl", classOf[LocalGsFileSystem].getName)
    conf.setBoolean("fs.s3a.impl.disable.cache", true)
    conf.set(LocalGsFileSystem.RootConfigKey, root.toString)
    conf.set(LocalGsFileSystem.ExpectedAccessKeyConfigKey, "test-access")
    conf.set(LocalGsFileSystem.ExpectedSecretKeyConfigKey, "test-secret")
    conf.set(LocalGsFileSystem.ExpectedRegionConfigKey, "us-west-2")
    conf
  }

  private def failingRenameHadoopConf(root: Path): Configuration = {
    val conf = hadoopConf(root)
    conf.setBoolean(LocalGsFileSystem.FailRenameConfigKey, true)
    conf
  }

  private class TestManager(
    cfg: MigratorConfig,
    processed: Set[String],
    conf: Configuration,
    redactionRegex: Option[String] = None
  ) extends SavepointsManager(cfg, Some(conf), redactionRegex) {
    override def describeMigrationState(): String = s"Processed: ${processed.size}"
    override def updateConfigWithMigrationState(): MigratorConfig =
      cfg.copy(skipParquetFiles = Some(processed))
  }

  private def deleteRecursively(dir: Path): Unit =
    if (Files.exists(dir)) {
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }

  test("savepoints can be written to and resumed from a gs:// Hadoop filesystem") {
    val root = Files.createTempDirectory("savepoints-gs")
    try {
      val conf = hadoopConf(root)
      val savepointsPath = "gs://bucket-a/migrator/savepoints"
      val cfg = newConfig(savepointsPath)
      val manager = new TestManager(cfg, Set("file-a.parquet"), conf)

      try manager.dumpMigrationState("test")
      finally manager.close()

      val localSavepointsDir = root.resolve("bucket-a").resolve("migrator").resolve("savepoints")
      val savepointFiles = Using.resource(Files.list(localSavepointsDir)) { stream =>
        stream.toArray
          .map(_.asInstanceOf[Path])
          .filter(_.getFileName.toString.endsWith(".yaml"))
      }

      assertEquals(savepointFiles.length, 1)

      val savepointUri = s"${savepointsPath}/${savepointFiles.head.getFileName}"
      val resumed = MigratorConfig.loadFrom(savepointUri, conf)
      assertEquals(resumed.skipParquetFiles.getOrElse(Set.empty), Set("file-a.parquet"))
    } finally deleteRecursively(root)
  }

  test("gs:// savepoint paths with spaces are handled by Hadoop instead of local Path") {
    val root = Files.createTempDirectory("savepoints-gs-spaces")
    try {
      val conf = hadoopConf(root)
      val savepointsPath = "gs://bucket-a/migrator/savepoints with spaces"
      val cfg = newConfig(savepointsPath)
      val manager = new TestManager(cfg, Set("file with spaces.parquet"), conf)

      try manager.dumpMigrationState("test")
      finally manager.close()

      val localSavepointsDir =
        root.resolve("bucket-a").resolve("migrator").resolve("savepoints with spaces")
      val savepointFiles = Using.resource(Files.list(localSavepointsDir)) { stream =>
        stream.toArray
          .map(_.asInstanceOf[Path])
          .filter(_.getFileName.toString.endsWith(".yaml"))
      }

      assertEquals(savepointFiles.length, 1)

      val savepointUri = s"${savepointsPath}/${savepointFiles.head.getFileName}"
      val resumed = MigratorConfig.loadFrom(savepointUri, conf)
      assertEquals(
        resumed.skipParquetFiles.getOrElse(Set.empty),
        Set("file with spaces.parquet")
      )
    } finally deleteRecursively(root)
  }

  test("gcs savepoint target applies configured credentials and project id to Hadoop") {
    val root = Files.createTempDirectory("savepoints-gcs-credentials")
    try {
      val conf = gcsHadoopConf(root)
      val target = SavepointsTarget.GCS(
        bucket      = "bucket-a",
        prefix      = Some("migrator/savepoints"),
        projectId   = Some("scylla-migrator-test"),
        credentials = Some(SavepointsTarget.GCSCredentials("/etc/gcp/key.json"))
      )
      val cfg = newConfig(target.storagePath).copy(
        savepoints = Savepoints(
          intervalSeconds = 3600,
          path            = target.storagePath,
          target          = Some(target)
        )
      )
      val manager = new TestManager(cfg, Set("file-a.parquet"), conf)

      try manager.dumpMigrationState("test")
      finally manager.close()

      val localSavepointsDir = root.resolve("bucket-a").resolve("migrator").resolve("savepoints")
      val savepointFiles = Using.resource(Files.list(localSavepointsDir)) { stream =>
        stream.toArray
          .map(_.asInstanceOf[Path])
          .filter(_.getFileName.toString.endsWith(".yaml"))
      }

      assertEquals(savepointFiles.length, 1)
    } finally deleteRecursively(root)
  }

  test("gcs savepoint target refuses credentials when Spark redaction misses keyfile") {
    val root = Files.createTempDirectory("savepoints-gcs-redaction")
    try {
      val conf = hadoopConf(root)
      val target = SavepointsTarget.GCS(
        bucket      = "bucket-a",
        prefix      = Some("migrator/savepoints"),
        credentials = Some(SavepointsTarget.GCSCredentials("/etc/gcp/key.json"))
      )
      val cfg = newConfig(target.storagePath).copy(
        savepoints = Savepoints(
          intervalSeconds = 3600,
          path            = target.storagePath,
          target          = Some(target)
        )
      )

      val error = intercept[IllegalArgumentException] {
        new TestManager(cfg, Set.empty, conf, Some("(?i)token"))
      }

      assert(
        error.getMessage.contains("fs.gs.auth.service.account.json.keyfile"),
        s"expected GCS keyfile redaction guard, got: ${error.getMessage}"
      )
    } finally deleteRecursively(root)
  }

  test("failed Hadoop rename does not delete an existing destination object") {
    val root = Files.createTempDirectory("savepoints-gs-rename-failure")
    try {
      val conf = failingRenameHadoopConf(root)
      val path = "gs://bucket-a/migrator/savepoints/savepoint-existing.yaml"
      val localPath = root
        .resolve("bucket-a")
        .resolve("migrator")
        .resolve("savepoints")
        .resolve("savepoint-existing.yaml")
      Files.createDirectories(localPath.getParent)
      Files.write(localPath, "old-savepoint".getBytes(java.nio.charset.StandardCharsets.UTF_8))

      val error = intercept[IOException] {
        PathIO
          .forPath(path, Some(conf))
          .writeUtf8Atomically(
            path,
            "new-savepoint".getBytes(java.nio.charset.StandardCharsets.UTF_8)
          )
      }

      assert(
        error.getMessage.contains("Failed to rename"),
        s"unexpected failure: ${error.getMessage}"
      )
      assert(
        Files.exists(localPath),
        "the previous destination object must survive a failed Hadoop rename"
      )
      assertEquals(
        new String(Files.readAllBytes(localPath), java.nio.charset.StandardCharsets.UTF_8),
        "old-savepoint"
      )
    } finally deleteRecursively(root)
  }

  test("unsupported s3a savepoint paths report S3A connector guidance") {
    val conf = new Configuration(false)

    val error = intercept[IllegalArgumentException] {
      PathIO.forPath("s3a://bucket-a/migrator/savepoints", Some(conf))
    }

    assert(
      error.getMessage.contains("Hadoop AWS/S3A connector"),
      s"expected S3A connector guidance, got: ${error.getMessage}"
    )
    assert(
      !error.getMessage.contains("GCS connector"),
      s"unexpected GCS guidance for an S3A path: ${error.getMessage}"
    )
  }

  test("s3 savepoint target applies configured credentials and region to Hadoop") {
    val root = Files.createTempDirectory("savepoints-s3-credentials")
    try {
      val conf = s3HadoopConf(root)
      val target = SavepointsTarget.S3(
        bucket      = "bucket-a",
        prefix      = Some("migrator/savepoints"),
        region      = Some("us-west-2"),
        credentials = Some(ConfigAWSCredentials("test-access", "test-secret", None))
      )
      val cfg = newConfig(target.storagePath).copy(
        savepoints = Savepoints(
          intervalSeconds = 3600,
          path            = target.storagePath,
          target          = Some(target)
        )
      )
      val manager = new TestManager(cfg, Set("file-a.parquet"), conf)

      try manager.dumpMigrationState("test")
      finally manager.close()

      val localSavepointsDir = root.resolve("bucket-a").resolve("migrator").resolve("savepoints")
      val savepointFiles = Using.resource(Files.list(localSavepointsDir)) { stream =>
        stream.toArray
          .map(_.asInstanceOf[Path])
          .filter(_.getFileName.toString.endsWith(".yaml"))
      }

      assertEquals(savepointFiles.length, 1)
    } finally deleteRecursively(root)
  }

  test("s3 savepoint target credentials override inherited Hadoop credentials provider") {
    val root = Files.createTempDirectory("savepoints-s3-provider-override")
    try {
      val conf = s3HadoopConf(root)
      conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
      )
      conf.set("fs.s3a.session.token", "stale-session-token")
      conf.set(
        LocalGsFileSystem.ExpectedCredentialsProviderConfigKey,
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
      conf.setBoolean(LocalGsFileSystem.ForbidSessionTokenConfigKey, true)

      val target = SavepointsTarget.S3(
        bucket      = "bucket-a",
        prefix      = Some("migrator/savepoints"),
        region      = Some("us-west-2"),
        credentials = Some(ConfigAWSCredentials("test-access", "test-secret", None))
      )
      val cfg = newConfig(target.storagePath).copy(
        savepoints = Savepoints(
          intervalSeconds = 3600,
          path            = target.storagePath,
          target          = Some(target)
        )
      )
      val manager = new TestManager(cfg, Set("file-a.parquet"), conf)

      try manager.dumpMigrationState("test")
      finally manager.close()

      val localSavepointsDir = root.resolve("bucket-a").resolve("migrator").resolve("savepoints")
      val savepointFiles = Using.resource(Files.list(localSavepointsDir)) { stream =>
        stream.toArray
          .map(_.asInstanceOf[Path])
          .filter(_.getFileName.toString.endsWith(".yaml"))
      }

      assertEquals(savepointFiles.length, 1)
    } finally deleteRecursively(root)
  }

  test("s3 savepoint target refuses credentials when Spark redaction misses keys") {
    val root = Files.createTempDirectory("savepoints-s3-redaction")
    try {
      val conf = s3HadoopConf(root)
      val target = SavepointsTarget.S3(
        bucket      = "bucket-a",
        prefix      = Some("migrator/savepoints"),
        region      = Some("us-west-2"),
        credentials = Some(ConfigAWSCredentials("test-access", "test-secret", None))
      )
      val cfg = newConfig(target.storagePath).copy(
        savepoints = Savepoints(
          intervalSeconds = 3600,
          path            = target.storagePath,
          target          = Some(target)
        )
      )

      val error = intercept[IllegalArgumentException] {
        new TestManager(cfg, Set.empty, conf, Some("(?i)token"))
      }

      assert(
        error.getMessage.contains("fs.s3a.access.key") &&
          error.getMessage.contains("fs.s3a.secret.key"),
        s"expected S3A credential redaction guard, got: ${error.getMessage}"
      )
    } finally deleteRecursively(root)
  }
}

object LocalGsFileSystem {
  val RootConfigKey = "scylla.migrator.test.gs.root"
  val FailRenameConfigKey = "scylla.migrator.test.gs.fail-rename"
  val ExpectedGcsAuthTypeConfigKey = "scylla.migrator.test.expected.fs.gs.auth.type"
  val ExpectedGcsJsonKeyfileConfigKey =
    "scylla.migrator.test.expected.fs.gs.auth.service.account.json.keyfile"
  val ExpectedGcsProjectIdConfigKey = "scylla.migrator.test.expected.fs.gs.project.id"
  val ExpectedAccessKeyConfigKey = "scylla.migrator.test.expected.fs.s3a.access.key"
  val ExpectedSecretKeyConfigKey = "scylla.migrator.test.expected.fs.s3a.secret.key"
  val ExpectedRegionConfigKey = "scylla.migrator.test.expected.fs.s3a.endpoint.region"
  val ExpectedCredentialsProviderConfigKey =
    "scylla.migrator.test.expected.fs.s3a.aws.credentials.provider"
  val ForbidSessionTokenConfigKey =
    "scylla.migrator.test.forbid.fs.s3a.session.token"
}

class LocalGsFileSystem extends FileSystem {
  private val local = new RawLocalFileSystem()
  private var root: Path = _
  private var fsUri: URI = _
  private var workingDirectory: HadoopPath = _
  private var failRename: Boolean = false

  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    val configuredRoot = Option(conf.get(LocalGsFileSystem.RootConfigKey))
      .getOrElse(throw new IOException(s"Missing ${LocalGsFileSystem.RootConfigKey}"))
    root = java.nio.file.Paths.get(configuredRoot)
    Files.createDirectories(root)
    local.initialize(root.toUri, conf)
    requireExpected(conf, "fs.gs.auth.type", LocalGsFileSystem.ExpectedGcsAuthTypeConfigKey)
    requireExpected(
      conf,
      "fs.gs.auth.service.account.json.keyfile",
      LocalGsFileSystem.ExpectedGcsJsonKeyfileConfigKey
    )
    requireExpected(conf, "fs.gs.project.id", LocalGsFileSystem.ExpectedGcsProjectIdConfigKey)
    requireExpected(conf, "fs.s3a.access.key", LocalGsFileSystem.ExpectedAccessKeyConfigKey)
    requireExpected(conf, "fs.s3a.secret.key", LocalGsFileSystem.ExpectedSecretKeyConfigKey)
    requireExpected(conf, "fs.s3a.endpoint.region", LocalGsFileSystem.ExpectedRegionConfigKey)
    requireExpected(
      conf,
      "fs.s3a.aws.credentials.provider",
      LocalGsFileSystem.ExpectedCredentialsProviderConfigKey
    )
    requireAbsent(conf, "fs.s3a.session.token", LocalGsFileSystem.ForbidSessionTokenConfigKey)
    fsUri            = new URI(name.getScheme, name.getAuthority, null, null, null)
    workingDirectory = new HadoopPath(s"${fsUri.toString}/")
    failRename       = conf.getBoolean(LocalGsFileSystem.FailRenameConfigKey, false)
  }

  override def getUri: URI = fsUri

  override def open(f: HadoopPath, bufferSize: Int): FSDataInputStream =
    local.open(toLocalPath(f), bufferSize)

  override def create(
    f: HadoopPath,
    permission: FsPermission,
    overwrite: Boolean,
    bufferSize: Int,
    replication: Short,
    blockSize: Long,
    progress: Progressable
  ): FSDataOutputStream = {
    val localPath = toLocalNioPath(f)
    Option(localPath.getParent).foreach(Files.createDirectories(_))
    local.create(
      toLocalPath(f),
      permission,
      overwrite,
      bufferSize,
      replication,
      blockSize,
      progress
    )
  }

  override def append(
    f: HadoopPath,
    bufferSize: Int,
    progress: Progressable
  ): FSDataOutputStream =
    throw new UnsupportedOperationException("append is not supported")

  override def rename(src: HadoopPath, dst: HadoopPath): Boolean = {
    Option(toLocalNioPath(dst).getParent).foreach(Files.createDirectories(_))
    if (failRename) false
    else local.rename(toLocalPath(src), toLocalPath(dst))
  }

  override def delete(f: HadoopPath, recursive: Boolean): Boolean =
    local.delete(toLocalPath(f), recursive)

  override def listStatus(f: HadoopPath): Array[FileStatus] =
    local.listStatus(toLocalPath(f))

  override def setWorkingDirectory(newDir: HadoopPath): Unit =
    workingDirectory = newDir

  override def getWorkingDirectory: HadoopPath = workingDirectory

  override def mkdirs(f: HadoopPath, permission: FsPermission): Boolean =
    local.mkdirs(toLocalPath(f), permission)

  override def getFileStatus(f: HadoopPath): FileStatus =
    local.getFileStatus(toLocalPath(f))

  private def toLocalPath(path: HadoopPath): HadoopPath =
    new HadoopPath(toLocalNioPath(path).toUri)

  private def toLocalNioPath(path: HadoopPath): Path = {
    val uri = path.toUri
    val bucket = Option(uri.getAuthority).getOrElse(Option(fsUri.getAuthority).getOrElse("bucket"))
    val objectPath = Option(uri.getPath).getOrElse("").stripPrefix("/")
    root.resolve(bucket).resolve(objectPath)
  }

  private def requireExpected(
    conf: Configuration,
    hadoopKey: String,
    expectedValueKey: String
  ): Unit =
    Option(conf.get(expectedValueKey)).foreach { expected =>
      val actual = conf.get(hadoopKey)
      if (actual != expected)
        throw new IOException(
          s"Expected Hadoop config ${hadoopKey} to be ${expected}, got ${actual}"
        )
    }

  private def requireAbsent(conf: Configuration, hadoopKey: String, guardKey: String): Unit =
    if (conf.getBoolean(guardKey, false)) {
      val actual = conf.get(hadoopKey)
      if (actual != null)
        throw new IOException(s"Expected Hadoop config ${hadoopKey} to be absent, got ${actual}")
    }
}
