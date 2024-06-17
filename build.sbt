import sbt.librarymanagement.InclExclRule

val awsSdkVersion = "1.11.728"
val sparkVersion = "2.4.4"
val hadoopVersion = "2.6.5"
val dynamodbStreamsKinesisAdapterVersion = "1.5.2"

inThisBuild(
  List(
    organization := "com.scylladb",
    scalaVersion := "2.11.12",
    scalacOptions += "-target:jvm-1.8"
  )
)

// Augmentation of spark-streaming-kinesis-asl to also work with DynamoDB Streams
lazy val `spark-kinesis-dynamodb` = project.in(file("spark-kinesis-dynamodb")).settings(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming-kinesis-asl"     % sparkVersion,
    "com.amazonaws"    % "dynamodb-streams-kinesis-adapter" % dynamodbStreamsKinesisAdapterVersion
  )
)

lazy val migrator = (project in file("migrator")).settings(
  name      := "scylla-migrator",
  version   := "0.0.1",
  mainClass := Some("com.scylladb.migrator.Migrator"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  javaOptions ++= Seq(
    "-Xms512M",
    "-Xmx2048M",
    "-XX:MaxPermSize=2048M",
    "-XX:+CMSClassUnloadingEnabled"),
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-Ypartial-unification"),
  Test / parallelExecution := false,
  fork                     := true,
  scalafmtOnCompile        := true,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming"      % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"            % sparkVersion % "provided",
    ("org.apache.hadoop" % "hadoop-aws"           % hadoopVersion)
      // Exclude dependencies to old versions of libraries that could create duplicates when we run sbt-assembly
      .excludeAll(
        InclExclRule("com.amazonaws", "aws-java-sdk"),
        InclExclRule("asm", "asm"),
        InclExclRule("com.sun.jersey", "jersey-core"),
        InclExclRule("com.sun.jersey", "jersey-server"),
        InclExclRule("javax.servlet", "servlet-api"),
      ),
    "com.amazonaws"    % "aws-java-sdk-dynamodb" % awsSdkVersion,
    "com.amazonaws"    % "aws-java-sdk-s3"       % awsSdkVersion,
    "com.amazonaws"    % "aws-java-sdk-sts"      % awsSdkVersion,
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.0-3-g35e0c096",
    "com.amazon.emr" % "emr-dynamodb-hadoop" % "4.16.0",
    "io.circe"       %% "circe-generic"      % "0.11.1",
    "io.circe"       %% "circe-parser"       % "0.11.1",
    "io.circe"       %% "circe-yaml"         % "0.10.1",
  ),
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("org.yaml.snakeyaml.**" -> "com.scylladb.shaded.@1").inAll
  ),
  assembly / assemblyMergeStrategy := {
    // Handle duplicates between the transitive dependencies of Spark itself
    case PathList("org", "aopalliance", _*)                  => MergeStrategy.first
    case PathList("org", "apache", "commons", "logging", _*) => MergeStrategy.first
    case PathList("org", "apache", "hadoop", _*)             => MergeStrategy.first
    case PathList("org", "apache", "spark", _*)              => MergeStrategy.first
    case PathList("javax", "inject", _*)                     => MergeStrategy.first
    case "module-info.class"                                 => MergeStrategy.discard // Safe because Java 8 does not support modules
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  },
  // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
  Compile / run := Defaults
    .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
    .evaluated,
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  pomIncludeRepository := { x =>
    false
  },
  pomIncludeRepository := { x =>
    false
  },
  // publish settings
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
).dependsOn(`spark-kinesis-dynamodb`)

lazy val tests = project.in(file("tests")).settings(
  libraryDependencies ++= Seq(
    "com.amazonaws"            % "aws-java-sdk-dynamodb"     % awsSdkVersion,
    "org.apache.spark"         %% "spark-sql"                % sparkVersion,
    "org.apache.cassandra"     % "java-driver-query-builder" % "4.18.0",
    "com.github.mjakubowski84" %% "parquet4s-core"           % "1.9.4",
    "org.apache.hadoop"        % "hadoop-client"             % hadoopVersion,
    "org.scalameta"            %% "munit"                    % "0.7.29",
    "org.scala-lang.modules"   %% "scala-collection-compat"  % "2.11.0"
  ),
  Test / parallelExecution := false
).dependsOn(migrator)

lazy val root = project.in(file("."))
  .aggregate(migrator, `spark-kinesis-dynamodb`, tests)
