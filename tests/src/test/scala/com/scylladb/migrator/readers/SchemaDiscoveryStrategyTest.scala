package com.scylladb.migrator.readers

import com.scylladb.migrator.config.SchemaDiscoveryStrategy

class SchemaDiscoveryStrategyTest extends munit.FunSuite {

  test("SchemaDiscoveryStrategy: decode progressive") {
    assertEquals(
      io.circe.parser.decode[SchemaDiscoveryStrategy]("\"progressive\""),
      Right(SchemaDiscoveryStrategy.Progressive)
    )
  }

  test("SchemaDiscoveryStrategy: decode single") {
    assertEquals(
      io.circe.parser.decode[SchemaDiscoveryStrategy]("\"single\""),
      Right(SchemaDiscoveryStrategy.Single)
    )
  }

  test("SchemaDiscoveryStrategy: decode full") {
    assertEquals(
      io.circe.parser.decode[SchemaDiscoveryStrategy]("\"full\""),
      Right(SchemaDiscoveryStrategy.Full)
    )
  }

  test("SchemaDiscoveryStrategy: decode invalid") {
    assert(
      io.circe.parser.decode[SchemaDiscoveryStrategy]("\"unknown\"").isLeft
    )
  }

  test("SchemaDiscoveryStrategy: encode round-trip") {
    import io.circe.syntax._
    val strategy: SchemaDiscoveryStrategy = SchemaDiscoveryStrategy.Progressive
    val json = strategy.asJson
    assertEquals(
      json.as[SchemaDiscoveryStrategy],
      Right(SchemaDiscoveryStrategy.Progressive)
    )
  }
}
