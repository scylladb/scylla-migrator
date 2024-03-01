import sbt.librarymanagement.InclExclRule

val awsSdkVersion = "1.11.728"
val sparkVersion = "2.4.4"

lazy val migrator = (project in file("migrator")).settings(
  inThisBuild(
    List(
      organization := "com.scylladb",
      scalaVersion := "2.11.12"
    )),
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
  parallelExecution in Test := false,
  fork                      := true,
  scalafmtOnCompile         := true,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming"      % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"            % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"            % sparkVersion % "provided",
    "com.amazonaws"    % "aws-java-sdk-sts"      % awsSdkVersion,
    "com.amazonaws"    % "aws-java-sdk-dynamodb" % awsSdkVersion,
    ("com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.5.2")
      .excludeAll(InclExclRule("com.fasterxml.jackson.core")),
    "com.amazon.emr" % "emr-dynamodb-hadoop" % "4.8.0",
    "org.yaml"       % "snakeyaml"      % "1.23",
    "io.circe"       %% "circe-yaml"    % "0.9.0",
    "io.circe"       %% "circe-generic" % "0.9.0",
  ),
  assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("org.yaml.snakeyaml.**" -> "com.scylladb.shaded.@1").inAll
  ),
  assemblyMergeStrategy in assembly := {
    case PathList("org", "joda", "time", _ @_*)                       => MergeStrategy.first
    case PathList("org", "apache", "commons", "logging", _ @_*)       => MergeStrategy.first
    case PathList("com", "fasterxml", "jackson", "annotation", _ @_*) => MergeStrategy.first
    case PathList("com", "fasterxml", "jackson", "core", _ @_*)       => MergeStrategy.first
    case PathList("com", "fasterxml", "jackson", "databind", _ @_*)   => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
  run in Compile := Defaults
    .runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
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
)

lazy val tests = project.in(file("tests")).settings(
  libraryDependencies ++= Seq(
    "com.amazonaws" % "aws-java-sdk-dynamodb" % awsSdkVersion,
    "org.scalameta" %% "munit" % "0.7.29",
    "org.scala-lang.modules" %% "scala-collection-compat" % "2.11.0"
  ),
  testFrameworks += new TestFramework("munit.Framework"),
  Test / parallelExecution := false
)

lazy val root = project.in(file("."))
  .aggregate(migrator, tests)
