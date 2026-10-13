package com.scylladb.migrator.readers

import com.aerospike.client.{ Key, Record }
import com.scylladb.migrator.config.{ AerospikeInvalidRecordHandling, SourceSettings }
import io.circe.Json
import org.apache.spark.sql.types._

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

class AerospikeSchemaValidationTest extends munit.FunSuite {
  private val config = Json.obj(
    "type"      -> Json.fromString("aerospike"),
    "keyMode"   -> Json.fromString("string"),
    "hosts"     -> Json.arr(Json.fromString("unreachable.invalid")),
    "namespace" -> Json.fromString("ns"),
    "set"       -> Json.fromString("set"),
    "schema"    -> Json.obj()
  )
  private val source = config.as[SourceSettings].toOption.get.asInstanceOf[SourceSettings.Aerospike]
  private val withMetadata = source.copy(preserveTTL = Some(true), preserveGeneration = Some(true))

  private def readConfig(settings: SourceSettings.Aerospike) = AerospikeReadConfig(
    settings.keyMode,
    settings.namespace,
    settings.set,
    1,
    1,
    1,
    0,
    1,
    settings.keyColumnName,
    Option.when(settings.ttlEnabled)(settings.ttlColumnName),
    Option.when(settings.generationEnabled)(settings.generationColumnName),
    settings.invalidRecordHandling
  )

  private def convert(settings: SourceSettings.Aerospike, bins: (String, Object)*) =
    AerospikeRDD.recordToRow(
      new Key("ns", "set", "id"),
      new Record(bins.toMap.asJava, 99, 0),
      Aerospike.buildSchema(settings),
      readConfig(settings),
      0L
    )

  test("schema is a required mapping, with explicit empty projection supported") {
    assert(config.mapObject(_.remove("schema")).as[SourceSettings].isLeft)
    for (invalid <- Seq(Json.Null, Json.arr(), Json.fromString("value")))
      assert(config.mapObject(_.add("schema", invalid)).as[SourceSettings].isLeft)
    val empty = Aerospike.buildSchema(source)
    assertEquals(empty.fieldNames.toSeq, Seq("aero_key"))
    assertEquals(convert(source, "undeclared" -> "ignored").get.toSeq, Seq("id"))
    assertEquals(
      Aerospike.buildSchema(withMetadata).fieldNames.toSeq,
      Seq("aero_key", "aero_ttl", "aero_generation")
    )
  }

  test("schema preserves declared order and sparse bins remain nullable") {
    val declared = Json.obj(
      "z" -> Json.fromString("string"),
      "a" -> Json.fromString("long"),
      "y" -> Json.fromString("double"),
      "b" -> Json.fromString("boolean"),
      "x" -> Json.fromString("binary")
    )
    val settings = config
      .mapObject(_.add("schema", declared))
      .as[SourceSettings]
      .toOption
      .get
      .asInstanceOf[SourceSettings.Aerospike]
    val schema = Aerospike.buildSchema(settings)
    assertEquals(schema.fieldNames.toSeq, Seq("aero_key", "z", "a", "y", "b", "x"))
    assert(schema.fields.tail.forall(_.nullable))
    assertEquals(
      convert(settings, "z" -> "present", "extra" -> "ignored").get.toSeq,
      Seq("id", "present", null, null, null, null)
    )
    assertEquals(SourceSettings.encoder(settings).as[SourceSettings], Right(settings))
  }

  test("invalid schema names and types fail before any connection") {
    for (
      schema <- Seq(
                  ListMap(" "        -> "long"),
                  ListMap("aero_key" -> "long"),
                  ListMap("AERO_KEY" -> "long"),
                  ListMap("name"     -> "string", "NAME" -> "string"),
                  ListMap("value"    -> "unsupported")
                )
    ) {
      intercept[IllegalArgumentException](Aerospike.buildSchema(source.copy(schema = schema)))
      val jsonSchema = Json.obj(schema.toSeq.map { case (name, value) =>
        name -> Json.fromString(value)
      }: _*)
      assert(config.mapObject(_.add("schema", jsonSchema)).as[SourceSettings].isLeft)
    }
  }

  test("invalidRecordHandling defaults to skip and round-trips both policies") {
    assertEquals(source.invalidRecordHandling, AerospikeInvalidRecordHandling.Skip)
    for (
      (text, policy) <- Seq(
                          "skip" -> AerospikeInvalidRecordHandling.Skip,
                          "null" -> AerospikeInvalidRecordHandling.Null
                        )
    ) {
      val settings = config
        .mapObject(_.add("invalidRecordHandling", Json.fromString(text)))
        .as[SourceSettings]
        .toOption
        .get
        .asInstanceOf[SourceSettings.Aerospike]
      assertEquals(settings.invalidRecordHandling, policy)
      assertEquals(SourceSettings.encoder(settings).as[SourceSettings], Right(settings))
    }
    for (invalid <- Seq(Json.Null, Json.fromString("unknown"), Json.fromInt(1)))
      assert(config.mapObject(_.add("invalidRecordHandling", invalid)).as[SourceSettings].isLeft)
  }

  test("YAML null policy must be quoted and explicit YAML null is rejected") {
    def decode(policy: String) = io.circe.yaml.parser
      .parse(s"""
                |type: aerospike
                |keyMode: string
                |hosts: [unreachable.invalid]
                |namespace: ns
                |set: set
                |schema: {}
                |invalidRecordHandling: $policy
                |""".stripMargin)
      .flatMap(_.as[SourceSettings])
    assert(decode("null").isLeft)
    assertEquals(
      decode("\"null\"").toOption.get.asInstanceOf[SourceSettings.Aerospike].invalidRecordHandling,
      AerospikeInvalidRecordHandling.Null
    )
  }

  test("disabled metadata leaves its names available to ordinary bins") {
    val settings =
      source.copy(schema = ListMap("aero_ttl" -> "string", "aero_generation" -> "long"))
    assertEquals(
      Aerospike.buildSchema(settings).fieldNames.toSeq,
      Seq("aero_key", "aero_ttl", "aero_generation")
    )
    assertEquals(
      convert(settings, "aero_ttl" -> "source TTL", "aero_generation" -> Long.box(42)).get.toSeq,
      Seq("id", "source TTL", 42L)
    )
  }

  test("custom generated names preserve bins bearing default generated names") {
    val settings = withMetadata.copy(
      keyColumn        = Some("record_id"),
      ttlColumn        = Some("remaining"),
      generationColumn = Some("version"),
      schema = ListMap("aero_key" -> "long", "aero_ttl" -> "string", "aero_generation" -> "long")
    )
    assertEquals(
      Aerospike.buildSchema(settings).fieldNames.toSeq,
      Seq("record_id", "aero_key", "aero_ttl", "aero_generation", "remaining", "version")
    )
    assertEquals(
      convert(
        settings,
        "aero_key"        -> Long.box(7),
        "aero_ttl"        -> "source TTL",
        "aero_generation" -> Long.box(42)
      ).get.toSeq,
      Seq("id", 7L, "source TTL", 42L, -1, 99)
    )
    assertEquals(SourceSettings.encoder(settings).as[SourceSettings], Right(settings))
  }

  test("generated names reject empty settings and collisions ignoring case") {
    for (setting <- Seq("keyColumn", "ttlColumn", "generationColumn"))
      assert(config.mapObject(_.add(setting, Json.fromString("  "))).as[SourceSettings].isLeft)
    intercept[IllegalArgumentException](
      Aerospike.buildSchema(withMetadata.copy(keyColumn = Some("ID"), ttlColumn = Some("id")))
    )
    intercept[IllegalArgumentException](
      Aerospike.buildSchema(withMetadata.copy(schema = ListMap("aero_ttl" -> "long")))
    )
    Aerospike.validateGeneratedColumnNames(source.copy(ttlColumn = Some("aero_key")))
  }

  test("invalid fields skip records or become null while retaining valid fields") {
    val settings = source.copy(schema = ListMap("good" -> "string", "bad" -> "long"))
    assertEquals(convert(settings, "good" -> "retained", "bad" -> "invalid"), None)
    assertEquals(
      convert(
        settings.copy(invalidRecordHandling = AerospikeInvalidRecordHandling.Null),
        "good" -> "retained",
        "bad"  -> "invalid"
      ).get.toSeq,
      Seq("id", "retained", null)
    )
    for (policy <- Seq(AerospikeInvalidRecordHandling.Skip, AerospikeInvalidRecordHandling.Null))
      assertEquals(
        convert(settings.copy(invalidRecordHandling = policy), "good" -> "retained").get.toSeq,
        Seq("id", "retained", null)
      )
  }
}
