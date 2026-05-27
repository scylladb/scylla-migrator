package com.scylladb.migrator

import java.nio.file.{ Files, Path, Paths }

import scala.sys.process.{ Process, ProcessLogger }

class DeploySparkClusterScriptTest extends munit.FunSuite {
  import DeploySparkClusterScriptTest.CommandResult

  private val repoRoot = findRepoRoot()
  private val script = repoRoot.resolve("deploy_spark_cluster.py")
  private val python = sys.env.getOrElse("PYTHON", "python3")
  private val missingPrivateKey =
    repoRoot.resolve("target/deploy-script-test/nonexistent-key")

  test("deploy_spark_cluster.py compiles with Python") {
    val result = runPython("-m", "py_compile", script.toString)

    assertEquals(result.exitCode, 0, result.output)
  }

  test("top-level help lists supported subcommands") {
    val result = runScript("--help")

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "deploy")
    assertOutputContains(result.output, "show")
    assertOutputContains(result.output, "run")
    assertOutputContains(result.output, "redeploy")
    assertOutputContains(result.output, "destroy")
    assertOutputContains(result.output, "Deploy and operate a Spark cluster")
  }

  test("deploy help documents required safe access CIDRs") {
    val result = runScript("deploy", "--help")

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--allowed-ssh-cidr")
    assertOutputContains(result.output, "--allowed-web-cidr")
    assertOutputContains(result.output, "--allow-public-access")
    assertOutputContains(result.output, "--owner-tag")
    assertOutputContains(result.output, "--insecure-ssh")
  }

  test("deploy requires explicit SSH and web access CIDRs") {
    val result = runScript("deploy", "--skip-ansible")

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--allowed-ssh-cidr")
    assertOutputContains(result.output, "--allowed-web-cidr")
  }

  test("deploy rejects public SSH CIDR unless explicitly allowed") {
    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allowed-ssh-cidr",
      "0.0.0.0/0",
      "--allowed-web-cidr",
      "203.0.113.10/32",
      "--ssh-private-key",
      missingPrivateKey.toString
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--allowed-ssh-cidr opens access to the public internet")
  }

  test("deploy rejects public web CIDR unless explicitly allowed") {
    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allowed-ssh-cidr",
      "203.0.113.10/32",
      "--allowed-web-cidr",
      "0.0.0.0/0",
      "--ssh-private-key",
      missingPrivateKey.toString
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--allowed-web-cidr opens access to the public internet")
  }

  test("deploy rejects non-IPv4 CIDRs") {
    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allowed-ssh-cidr",
      "2001:db8::/32",
      "--allowed-web-cidr",
      "203.0.113.10/32"
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "must be an IPv4 CIDR")
  }

  test("allow-public-access gates the public CIDR guard") {
    Files.deleteIfExists(missingPrivateKey)

    val result = runScript(
      "deploy",
      "--skip-ansible",
      "--allow-public-access",
      "--allowed-ssh-cidr",
      "0.0.0.0/0",
      "--allowed-web-cidr",
      "0.0.0.0/0",
      "--ssh-private-key",
      missingPrivateKey.toString
    )

    assertNotEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "SSH private key does not exist")
    assert(!result.output.contains("opens access to the public internet"), result.output)
  }

  test("run help documents migration and SSH safety options") {
    val result = runScript("run", "--help")

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "--migration-type")
    assertOutputContains(result.output, "--validator")
    assertOutputContains(result.output, "--insecure-ssh")
  }

  test("redeploy help documents inventory rerun and SSH safety options") {
    val result = runScript("redeploy", "--help")

    assertEquals(result.exitCode, 0, result.output)
    assertOutputContains(result.output, "Rerun Ansible")
    assertOutputContains(result.output, "--ssh-private-key")
    assertOutputContains(result.output, "--insecure-ssh")
  }

  private def runScript(args: String*): CommandResult =
    runPython((script.toString +: args): _*)

  private def runPython(args: String*): CommandResult = {
    val output = new StringBuilder
    val exitCode = Process(python +: args, repoRoot.toFile).!(
      ProcessLogger(output append _ append "\n")
    )
    CommandResult(exitCode, output.toString)
  }

  private def assertOutputContains(haystack: String, needle: String): Unit =
    assert(
      haystack.contains(needle),
      s"Expected output to contain '$needle'. Output:\n$haystack"
    )

  private def findRepoRoot(): Path = {
    val start = Paths.get("").toAbsolutePath
    Iterator
      .iterate(start)(_.getParent)
      .takeWhile(_ != null)
      .find(path => Files.isRegularFile(path.resolve("deploy_spark_cluster.py")))
      .getOrElse(fail(s"Could not find deploy_spark_cluster.py from $start"))
  }
}

object DeploySparkClusterScriptTest {
  private final case class CommandResult(exitCode: Int, output: String)
}
