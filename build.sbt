import sbt._
import sbt.Keys._

// =============================================================================
// ScyllaDB Migrator with MariaDB Support - Build Configuration
// =============================================================================

lazy val root = (project in file("."))
  .settings(
    name := "scylla-migrator",
    organization := "com.scylladb",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.13.12",
    
    // Spark compatibility
    libraryDependencies ++= Seq(
      // Spark core dependencies (provided by Spark cluster)
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
      
      // JSON parsing with Circe
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "io.circe" %% "circe-yaml" % "0.14.2",
      
      // ScyllaDB Spark Connector
      "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraConnectorVersion,
      
      // Logging
      "org.slf4j" % "slf4j-api" % "2.0.9",
      "ch.qos.logback" % "logback-classic" % "1.4.14" % "runtime",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalamock" %% "scalamock" % "5.2.0" % Test
    ),
    
    // Assembly settings for fat JAR
    assembly / assemblyJarName := s"scylla-migrator-assembly-${version.value}.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case x if x.endsWith(".proto") => MergeStrategy.first
      case x if x.endsWith(".properties") => MergeStrategy.first
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    
    // Exclude Spark and Hadoop from fat JAR (provided by cluster)
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { jar =>
        val name = jar.data.getName
        name.startsWith("spark-") || 
        name.startsWith("hadoop-") ||
        name.startsWith("scala-library")
      }
    },
    
    // Scala compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Xlint:_",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen"
    ),
    
    // JVM options
    javaOptions ++= Seq(
      "-Xmx4g",
      "-XX:+UseG1GC"
    ),
    
    // Native library configuration
    // The JNI library must be in java.library.path at runtime
    fork := true,
    javaOptions += s"-Djava.library.path=${baseDirectory.value}/native/build"
  )

// =============================================================================
// Version Constants
// =============================================================================

lazy val sparkVersion = "3.5.1"
lazy val circeVersion = "0.14.6"
lazy val sparkCassandraConnectorVersion = "3.5.0"

// =============================================================================
// Custom Tasks
// =============================================================================

// Task to build native library
lazy val buildNative = taskKey[Unit]("Build native C++ library")
buildNative := {
  import scala.sys.process._
  val nativeDir = baseDirectory.value / "native"
  val buildDir = nativeDir / "build"
  
  // Create build directory
  buildDir.mkdirs()
  
  // Run CMake and Make
  val cmakeResult = Process(
    Seq("cmake", "-DCMAKE_BUILD_TYPE=Release", ".."),
    buildDir
  ).!
  
  if (cmakeResult != 0) {
    throw new RuntimeException("CMake configuration failed")
  }
  
  val makeResult = Process(
    Seq("make", "-j4"),
    buildDir
  ).!
  
  if (makeResult != 0) {
    throw new RuntimeException("Native library build failed")
  }
  
  streams.value.log.info("Native library built successfully")
}

// Ensure native library is built before assembly
assembly := (assembly dependsOn buildNative).value

// =============================================================================
// Project Layout
// =============================================================================

// migrator/
//   src/main/scala/
//     com/scylladb/migrator/
//       Migrator.scala                    - Main entry point
//       MariaDBMigrator.scala             - MariaDB migration logic
//       config/
//         MariaDBSourceSettings.scala     - MariaDB config types
//         MigratorConfig.scala            - Main config parser
//         SourceSettings.scala            - Source type definitions
//       mariadb/
//         NativeBridge.scala              - JNI bindings
//   src/test/scala/
//     com/scylladb/migrator/
//       MariaDBMigratorSpec.scala         - Tests
// 
// native/
//   include/
//     mariadb_scylla_migrator.h           - Main C++ API
//     mariadb_scylla_migrator_jni.h       - JNI declarations
//   src/
//     mariadb_connection.cpp              - MariaDB operations
//     scylladb_connection.cpp             - ScyllaDB operations
//     migrator.cpp                        - Orchestration
//     jni_bridge.cpp                      - JNI implementations
//   CMakeLists.txt                        - Build configuration
import sbt.librarymanagement.InclExclRule

// The AWS SDK version, the Spark version, and the Hadoop version must be compatible together
val awsSdkVersion = "2.23.19"
val sparkVersion = "3.5.1"
val hadoopVersion = "3.3.4"
val dynamodbStreamsKinesisAdapterVersion = "1.5.4" // Note This version still depends on AWS SDK 1.x, but there is no more recent version that supports AWS SDK v2.

inThisBuild(
  List(
    organization := "com.scylladb",
    scalaVersion := "2.13.14",
    scalacOptions ++= Seq("-release:8", "-deprecation", "-unchecked", "-feature"),
  )
)

// Augmentation of spark-streaming-kinesis-asl to also work with DynamoDB Streams
lazy val `spark-kinesis-dynamodb` = project.in(file("spark-kinesis-dynamodb")).settings(
  libraryDependencies ++= Seq(
    ("org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVersion)
      .excludeAll(InclExclRule("org.apache.spark", s"spark-streaming_${scalaBinaryVersion.value}")), // For some reason, the Spark dependency is not marked as provided in spark-streaming-kinesis-asl. We exclude it and then add it as provided.
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
    "com.amazonaws"    % "dynamodb-streams-kinesis-adapter" % dynamodbStreamsKinesisAdapterVersion
  )
)

lazy val migrator = (project in file("migrator")).enablePlugins(BuildInfoPlugin).settings(
  name      := "scylla-migrator",
  mainClass := Some("com.scylladb.migrator.Migrator"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  javaOptions ++= Seq(
    "-Xms512M",
    "-Xmx2048M",
    "-XX:MaxPermSize=2048M",
    "-XX:+CMSClassUnloadingEnabled"),
  Test / parallelExecution := false,
  fork                     := true,
  scalafmtOnCompile        := true,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming"      % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"            % sparkVersion % "provided",
    ("org.apache.hadoop" % "hadoop-aws"           % hadoopVersion) // Note: this package still depends on the AWS SDK v1
      // Exclude the AWS bundle because it creates many conflicts when generating the assembly
      .excludeAll(
        InclExclRule("com.amazonaws", "aws-java-sdk-bundle"),
      ),
    "software.amazon.awssdk"    % "s3-transfer-manager" % awsSdkVersion,
    "software.amazon.awssdk"    % "dynamodb" % awsSdkVersion,
    "software.amazon.awssdk"    % "s3"       % awsSdkVersion,
    "software.amazon.awssdk"    % "sts"      % awsSdkVersion,
    "com.scylladb" %% "spark-scylladb-connector" % "4.0.0",
    "com.github.jnr" % "jnr-posix" % "3.1.19", // Needed by the Spark ScyllaDB connector
    "com.scylladb.alternator" % "emr-dynamodb-hadoop" % "5.8.0",
    "com.scylladb.alternator" % "load-balancing" % "1.0.0",
    "io.circe"       %% "circe-generic"      % "0.14.7",
    "io.circe"       %% "circe-parser"       % "0.14.7",
    "io.circe"       %% "circe-yaml"         % "0.15.1",
  ),
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("org.yaml.snakeyaml.**" -> "com.scylladb.shaded.@1").inAll
  ),
  assembly / assemblyMergeStrategy := {
    // Handle duplicates between the transitive dependencies of Spark itself
    case "mime.types"                                             => MergeStrategy.first
    case PathList("META-INF", "io.netty.versions.properties")     => MergeStrategy.concat
    case PathList("META-INF", "versions", _, "module-info.class") => MergeStrategy.discard // OK as long as we don’t rely on Java 9+ features such as SPI
    case "module-info.class"                                      => MergeStrategy.discard // OK as long as we don’t rely on Java 9+ features such as SPI
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  assembly / assemblyJarName := s"${name.value}-assembly.jar",
  buildInfoKeys := Seq[BuildInfoKey](version),
  buildInfoPackage := "com.scylladb.migrator",
  // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
  Compile / run := Defaults
    .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
    .evaluated
).dependsOn(`spark-kinesis-dynamodb`)

lazy val tests = project.in(file("tests")).settings(
  libraryDependencies ++= Seq(
    "software.amazon.awssdk"    % "dynamodb"                 % awsSdkVersion,
    "org.apache.spark"         %% "spark-sql"                % sparkVersion,
    "org.apache.spark"         %% "spark-streaming"          % sparkVersion,
    "org.apache.cassandra"     % "java-driver-query-builder" % "4.18.0",
    "com.github.mjakubowski84" %% "parquet4s-core"           % "1.9.4",
    "org.apache.hadoop"        % "hadoop-client"             % hadoopVersion,
    "org.scalameta"            %% "munit"                    % "1.0.1"
  ),
  Test / parallelExecution := false,
  // Needed to build a Spark session on Java 17+, see https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct
  Test / javaOptions ++= {
    val maybeJavaMajorVersion =
      sys.props.get("java.version")
        .map(version => version.takeWhile(_ != '.').toInt)
    if (maybeJavaMajorVersion.exists(_ > 11))
      Seq("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
    else
      Nil
  },
  Test / fork := true,
).dependsOn(migrator)

lazy val root = project.in(file("."))
  .aggregate(migrator, `spark-kinesis-dynamodb`, tests)
