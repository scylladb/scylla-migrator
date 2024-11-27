import sbt.librarymanagement.InclExclRule
import java.text.SimpleDateFormat
import java.util.Date
// The AWS SDK version, the Spark version, and the Hadoop version must be compatible together
val awsSdkVersion = "2.23.19"
val sparkVersion = "3.5.0"
val hadoopVersion = "3.3.4"

inThisBuild(
  List(
    organization := "com.scylladb",
    scalaVersion := "2.12.18",
    scalacOptions ++= Seq("-release:8", "-deprecation", "-unchecked", "-feature"),
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
    "software.amazon.awssdk"    % "s3"       % awsSdkVersion,
    "software.amazon.awssdk"    % "sts"      % awsSdkVersion,
    "com.scylladb" %% "spark-scylladb-connector" % "4.0.0",
    "com.github.jnr" % "jnr-posix" % "3.1.19", // Needed by the Spark ScyllaDB connector
    "com.scylladb.alternator" % "load-balancing" % "1.0.0",
    "io.circe"       %% "circe-generic"      % "0.13.0",
    "io.circe"       %% "circe-parser"       % "0.13.0",
    "io.circe"       %% "circe-yaml"         % "0.13.0",
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

  assembly / assemblyJarName := {
    val dateFormat = new SimpleDateFormat("ddMMyyyy-HHmmss")
    val timestamp = dateFormat.format(new Date())
    s"${name.value}-$timestamp-assembly.jar"
  },
  buildInfoKeys := Seq[BuildInfoKey](version),
  buildInfoPackage := "com.scylladb.migrator",
  // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
  Compile / run := Defaults
    .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
    .evaluated
)

lazy val tests = project.in(file("tests")).settings(
  libraryDependencies ++= Seq(
    "org.apache.spark"         %% "spark-sql"                % sparkVersion,
    "com.github.mjakubowski84" %% "parquet4s-core"           % "1.9.4",
    /*
        "org.apache.cassandra"     % "java-driver-query-builder" % "4.18.0",
        "org.apache.hadoop"        % "hadoop-client"             % hadoopVersion,
    */
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
  .aggregate(migrator, tests)
