package com.scylladb.migrator.config

import io.circe.yaml

class AWSCredentialsParserTest extends munit.FunSuite {

  test("Basic authentication") {
    val config =
      """accessKey: foo
        |secretKey: bar""".stripMargin
    val expectedCredentials =
      AWSCredentials("foo", "bar", None)
    val parsedCredentials = parseCredentials(config)
    assertEquals(parsedCredentials, expectedCredentials)
  }

  test("Optional assume role") {
    val config =
      """accessKey: foo
        |secretKey: bar
        |assumeRole:
        |  arn: baz
        |  sessionName: bah""".stripMargin
    val expectedCredentials =
      AWSCredentials("foo", "bar", Some(AWSAssumeRole("baz", Some("bah"))))
    val parsedCredentials = parseCredentials(config)
    assertEquals(parsedCredentials, expectedCredentials)
  }

  private def parseCredentials(yamlContent: String): AWSCredentials =
    yaml.parser
      .parse(yamlContent)
      .flatMap(_.as[AWSCredentials])
      .getOrElse(fail("Failed to parse AWS credentials."))

}
