package com.scylladb.migrator.readers

import com.scylladb.migrator.config.{ AerospikeKeyMode, SourceSettings }
import io.circe.Json
import org.apache.spark.sql.types._

class AerospikeKeyModeTest extends munit.FunSuite {
  private val config = Json.obj(
    "type"      -> Json.fromString("aerospike"),
    "hosts"     -> Json.arr(Json.fromString("unreachable.invalid")),
    "namespace" -> Json.fromString("ns"),
    "set"       -> Json.fromString("set"),
    "schema"    -> Json.obj()
  )

  test("missing or invalid keyMode is rejected during configuration decoding") {
    assert(config.as[SourceSettings].isLeft)
    for (invalid <- Seq(Json.fromString("auto"), Json.fromString(""), Json.fromInt(42), Json.Null))
      assert(config.mapObject(_.add("keyMode", invalid)).as[SourceSettings].isLeft)
  }

  test("key modes round-trip and explicit bin schemas never override the key type") {
    val modes = Seq(
      ("string", AerospikeKeyMode.StringKey, StringType),
      ("integer", AerospikeKeyMode.IntegerKey, LongType),
      ("binary", AerospikeKeyMode.BinaryKey, BinaryType),
      ("digest", AerospikeKeyMode.Digest, StringType)
    )
    for {
      (name, mode, dataType) <- modes
      empty                  <- Seq(true, false)
    } {
      val bins = if (empty) Json.obj() else Json.obj("value" -> Json.fromString("string"))
      val json = config.mapObject(
        _.add("keyMode", Json.fromString(name))
          .add("schema", bins)
      )
      val source = json.as[SourceSettings].toOption.get.asInstanceOf[SourceSettings.Aerospike]
      assertEquals(source.keyMode, mode)
      assertEquals(SourceSettings.encoder(source).as[SourceSettings], Right(source))
      // No client is needed for an explicit schema, even when the bin list is empty.
      val schema = Aerospike.buildSchema(source)
      assertEquals(schema(Aerospike.KeyColumnName).dataType, dataType)
      assertEquals(schema(Aerospike.KeyColumnName).nullable, false)
      assertEquals(schema.fields.length, if (empty) 1 else 2)
    }
  }
}
