package com.scylladb.migrator.config

class ContactPointsTest extends munit.FunSuite {

  private def parse(hosts: String, defaultPort: Int = 9042) =
    ContactPoints.parse("Scylla target", hosts, defaultPort)

  private def assertError(hosts: String, expected: String): Unit =
    parse(hosts) match {
      case Left(errors)  => assert(errors.exists(_.contains(expected)), errors.mkString(" | "))
      case Right(points) => fail(s"Expected an error, got $points")
    }

  test("a single host uses the default port") {
    assertEquals(parse("scylla"), Right(List(ContactPoint("scylla", 9042))))
  }

  test("several hosts, some with their own port, the others with the default port") {
    assertEquals(
      parse("pe-az1.example.com:9142, pe-az2.example.com ,10.0.0.7:9143", defaultPort = 9200),
      Right(
        List(
          ContactPoint("pe-az1.example.com", 9142),
          ContactPoint("pe-az2.example.com", 9200),
          ContactPoint("10.0.0.7", 9143)
        )
      )
    )
  }

  test("IPv6 addresses: bracketed with or without a port, or bare without a port") {
    assertEquals(
      parse("[2001:db8::1]:9142,2001:db8::2,[2001:db8::3]"),
      Right(
        List(
          ContactPoint("2001:db8::1", 9142),
          ContactPoint("2001:db8::2", 9042),
          ContactPoint("2001:db8::3", 9042)
        )
      )
    )
  }

  test("rejects empty entries") {
    assertError("pe-az1.example.com,,pe-az2.example.com", "host must not be empty")
    assertError("pe-az1.example.com,", "host must not be empty")
  }

  test("rejects invalid ports") {
    assertError("pe-az1.example.com:notaport", "has an invalid port 'notaport'")
    assertError("pe-az1.example.com:70000", "port must be between 1 and 65535")
    assertError("[2001:db8::1]:", "has an invalid port ''")
  }

  test("rejects invalid hosts, reporting every bad entry") {
    parse("bad/host,good.example.com,[2001:db8::1]x") match {
      case Left(errors) =>
        assertEquals(errors.size, 2, errors.mkString(" | "))
        assert(errors.head.contains("Invalid Scylla target host"), errors.head)
        assert(errors(1).contains("expected '[address]' or '[address]:port'"), errors(1))
      case Right(points) => fail(s"Expected errors, got $points")
    }
  }
}
