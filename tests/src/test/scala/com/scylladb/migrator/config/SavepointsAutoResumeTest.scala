package com.scylladb.migrator.config

import io.circe.syntax._
import io.circe.yaml.parser
import io.circe.yaml.syntax._

class SavepointsAutoResumeTest extends munit.FunSuite {

  private def decode(yaml: String): Savepoints =
    parser
      .parse(yaml)
      .flatMap(_.as[Savepoints])
      .fold(throw _, identity)

  test("autoResume defaults to true when absent") {
    val savepoints = decode("""intervalSeconds: 300
                              |path: /tmp/savepoints
                              |""".stripMargin)
    assert(savepoints.autoResume)
  }

  test("autoResume can be disabled explicitly") {
    val savepoints = decode("""intervalSeconds: 300
                              |path: /tmp/savepoints
                              |autoResume: false
                              |""".stripMargin)
    assert(!savepoints.autoResume)
  }

  test("autoResume is omitted from the rendered output when left at the default") {
    val rendered = Savepoints(intervalSeconds = 300, path = "/tmp/savepoints").asJson.asYaml.spaces2
    assert(
      !rendered.contains("autoResume"),
      s"default autoResume must not appear in the rendered savepoints block:\n$rendered"
    )
  }

  test("autoResume is rendered when explicitly disabled and round-trips") {
    val savepoints =
      Savepoints(intervalSeconds = 300, path = "/tmp/savepoints", autoResume = false)
    val rendered = savepoints.asJson.asYaml.spaces2
    assert(rendered.contains("autoResume"), s"disabled autoResume must be rendered:\n$rendered")
    assertEquals(decode(rendered).autoResume, false)
  }
}
