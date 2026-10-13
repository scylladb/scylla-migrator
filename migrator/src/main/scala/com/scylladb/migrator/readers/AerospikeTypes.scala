package com.scylladb.migrator.readers

import com.scylladb.migrator.config.AerospikeKeyMode

import com.aerospike.client.Value.{ GeoJSONValue, HLLValue }
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.util.TypeUtils

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** Explicit schema conversion helpers for the Aerospike reader. */
object AerospikeTypes {
  private val log = LogManager.getLogger("com.scylladb.migrator.readers.AerospikeTypes")

  /** Track occurrence counts of unexpected types so we can log periodically rather than only once.
    * Bounded in practice: entries correspond to distinct Java class names, which are finite for any
    * given application.
    */
  private val warnedTypeCounts =
    new ConcurrentHashMap[String, java.util.concurrent.atomic.AtomicLong]()

  /** Interval at which repeated warnings are emitted for the same unrecognized type */
  private val WarnInterval = 10000L

  /** An intentional rejection of source data; unrelated failures must remain fatal. */
  private[migrator] final class InvalidValue(message: String) extends IllegalStateException(message)

  /** Convert a value according to its declared type, preserving exact values. Missing bins remain
    * null; an invalid collection member rejects its whole top-level bin.
    */
  private[migrator] def convertValue(value: Any, expectedType: DataType): Any =
    convertValue(value, expectedType, "value")

  private def convertValue(value: Any, expectedType: DataType, path: String): Any = {
    val converted = convertByRuntimeType(value, expectedType, path)
    if (converted == null || isCompatible(converted, expectedType)) converted
    else if (expectedType == StringType) converted.toString
    else
      throw new InvalidValue(
        s"Aerospike $path of type ${typeNameOf(value)} cannot be represented as $expectedType. " +
          "Provide a compatible declared schema type."
      )
  }

  /** Whether a converted value can be handed to Spark's row encoder for `expectedType`. */
  private def isCompatible(value: Any, expectedType: DataType): Boolean =
    (value, expectedType) match {
      case (_: java.lang.Long, LongType)               => true
      case (_: java.lang.Integer, IntegerType)         => true
      case (_: java.lang.Double, DoubleType)           => true
      case (_: java.lang.Boolean, BooleanType)         => true
      case (_: String, StringType)                     => true
      case (_: Array[Byte], BinaryType)                => true
      case (_: Seq[_], _: ArrayType)                   => true
      case (_: scala.collection.Map[_, _], _: MapType) => true
      case _                                           => false
    }

  private def typeNameOf(value: Any): String =
    if (value == null) "null" else value.getClass.getName

  private def convertCollectionMember(value: Any, expectedType: DataType, path: String): Any = {
    if (value == null)
      throw new InvalidValue(
        s"Aerospike $path is null, which CQL collections cannot store. " +
          "Use a string schema override to preserve the collection as text."
      )
    convertValue(value, expectedType, path)
  }

  /** Keep Java collection punctuation while rendering nested values by content. */
  private def renderCollectionMember(value: Any): String =
    if (value == null) "null" else convertValue(value, StringType).asInstanceOf[String]

  /** A double-to-long round trip alone misses Long.MaxValue because the JVM saturates it.
    * Constructing BigDecimal from the double checks its exact binary value instead.
    */
  private def exactDouble(value: java.lang.Long, path: String): java.lang.Double = {
    val converted = value.doubleValue()
    if (
      new java.math.BigDecimal(converted)
        .compareTo(java.math.BigDecimal.valueOf(value.longValue())) != 0
    )
      throw new InvalidValue(
        s"Aerospike $path integer cannot be represented exactly as DoubleType. " +
          "Use a string schema override to preserve mixed integer and double values."
      )
    java.lang.Double.valueOf(converted)
  }

  private def convertByRuntimeType(value: Any, expectedType: DataType, path: String): Any =
    value match {
      case null => null
      // Collection types first (most specific)
      case v: java.util.List[_] =>
        expectedType match {
          case ArrayType(et, _) =>
            v.asScala.iterator.zipWithIndex.map { case (element, index) =>
              convertCollectionMember(element, et, s"$path[$index]")
            }.toVector
          case StringType =>
            v.asScala.iterator.map(renderCollectionMember).mkString("[", ", ", "]")
          case _ =>
            v
        }
      case v: java.util.Map[_, _] =>
        expectedType match {
          case MapType(k, vt, _) =>
            val toCatalystKey = CatalystTypeConverters.createToCatalystConverter(k)
            val seenKeys = scala.collection.mutable.TreeSet.empty[Any](
              TypeUtils.getInterpretedOrdering(k)
            )
            v.asScala.iterator.zipWithIndex.map { case ((mk, mv), index) =>
              val convertedKey = convertCollectionMember(mk, k, s"$path.map key[$index]")
              if (!seenKeys.add(toCatalystKey(convertedKey)))
                throw new InvalidValue(
                  s"Distinct Aerospike map keys at $path become the same Spark key after conversion to $k"
                )
              convertedKey -> convertCollectionMember(mv, vt, s"$path.map value[$index]")
            }.toMap
          case StringType =>
            v.asScala.iterator
              .map { case (key, value) =>
                s"${renderCollectionMember(key)}=${renderCollectionMember(value)}"
              }
              .mkString("{", ", ", "}")
          case _ =>
            v
        }
      // Primitive types — handle StringType and DoubleType coercion inline
      case v: GeoJSONValue => v.toString
      case v: HLLValue =>
        convertByRuntimeType(v.getBytes, expectedType, path)
      case v: ByteBuffer =>
        val bytes = new Array[Byte](v.remaining())
        v.duplicate().get(bytes)
        convertByRuntimeType(bytes, expectedType, path)
      case v: Array[Byte] =>
        if (expectedType == StringType) hexFormat.formatHex(v) else v
      case v: java.lang.Integer =>
        expectedType match {
          case StringType => v.toString
          case DoubleType => java.lang.Double.valueOf(v.doubleValue())
          case _          => java.lang.Long.valueOf(v.longValue())
        }
      case v: java.lang.Float =>
        if (expectedType == StringType) v.toString
        else java.lang.Double.valueOf(v.doubleValue())
      case v: java.lang.Long =>
        expectedType match {
          case StringType => v.toString
          case DoubleType => exactDouble(v, path)
          case _          => v
        }
      case v: java.lang.Double =>
        if (expectedType == StringType) v.toString else v
      case v: String => v
      case v: java.lang.Boolean =>
        if (expectedType == BooleanType) v else v.toString
      // Catch-all: an unrecognized type is only representable as text. Warn on first occurrence
      // and then periodically (every 10,000 occurrences) per type to surface ongoing degradation.
      // For non-string columns the value is returned unchanged so the compatibility guard in
      // `convertValue` rejects it with a more specific error.
      case v =>
        if (expectedType == StringType) {
          val typeName = v.getClass.getName
          val counter = warnedTypeCounts.computeIfAbsent(
            typeName,
            _ => new java.util.concurrent.atomic.AtomicLong(0)
          )
          val count = counter.incrementAndGet()
          if (count == 1 || count % WarnInterval == 0)
            log.warn(
              s"convertValue: unexpected type $typeName for expected $expectedType, " +
                s"coercing to String ($count occurrences so far)"
            )
          v.toString
        } else v
    }

  /** Clear rate-limited warning state. Intended for tests to prevent state leaking across suites.
    */
  private[migrator] def reset(): Unit = warnedTypeCounts.clear()

  /** Fail fast if JDK < 17 on the driver before submitting tasks, and on each executor. */
  private[migrator] def requireJdk17(): Unit = { val _ = hexFormat }

  private lazy val hexFormat =
    try java.util.HexFormat.of()
    catch {
      case _: NoSuchMethodError | _: NoClassDefFoundError =>
        throw new UnsupportedOperationException(
          "Aerospike source requires JDK 17+. java.util.HexFormat is not available on this JVM " +
            s"(running ${System.getProperty("java.version")}). Please upgrade your JDK."
        )
    }

  /** The record key schema is independent of declared bin types. */
  private[migrator] def keyDataType(mode: AerospikeKeyMode): DataType = mode match {
    case AerospikeKeyMode.StringKey | AerospikeKeyMode.Digest => StringType
    case AerospikeKeyMode.IntegerKey                          => LongType
    case AerospikeKeyMode.BinaryKey                           => BinaryType
  }

  /** Validate every stored key without coercion or a digest fallback. */
  private[migrator] def extractKey(key: com.aerospike.client.Key, mode: AerospikeKeyMode): Any = {
    if (mode == AerospikeKeyMode.Digest) return hexFormat.formatHex(key.digest)
    if (key.userKey == null)
      throw new InvalidValue(
        "Aerospike record has no stored user key. Stored-key modes require sendKey=true on " +
          "every source record; use keyMode: digest to migrate records without stored keys."
      )
    val value = key.userKey.getObject
    val matches = mode match {
      case AerospikeKeyMode.StringKey  => value.isInstanceOf[String]
      case AerospikeKeyMode.IntegerKey => value.isInstanceOf[java.lang.Long]
      case AerospikeKeyMode.BinaryKey  => value.isInstanceOf[Array[Byte]]
      case AerospikeKeyMode.Digest     => false
    }
    if (!matches)
      throw new InvalidValue(
        s"Aerospike record key of type ${typeNameOf(value)} does not match configured " +
          s"keyMode ${AerospikeKeyMode.encoder(mode).noSpaces}. " +
          "Use keyMode: digest to migrate mixed key types."
      )
    value
  }

  /** Parse a user-provided type name into a Spark DataType. Supports scalar types (string, long,
    * double, boolean, binary) and their aliases from config-aerospike.yaml.example, plus collection
    * types (list<T>, map<K,V>). Collection types can be nested, e.g., list<map<string,long>>.
    */
  private[migrator] def parseType(typeName: String): DataType = {
    val lower = typeName.toLowerCase.trim
    lower match {
      case "string" | "text"  => StringType
      case "long" | "bigint"  => LongType
      case "int" | "integer"  => LongType // Aerospike stores all integers as Long
      case "double"           => DoubleType
      case "boolean" | "bool" => BooleanType
      case "binary" | "blob"  => BinaryType
      case s if s.startsWith("list<") && s.endsWith(">") =>
        val inner = s.substring(5, s.length - 1)
        ArrayType(parseType(inner), containsNull = true)
      case s if s.startsWith("map<") && s.endsWith(">") =>
        val inner = s.substring(4, s.length - 1)
        val (keyType, valueType) = splitMapTypes(inner)
        MapType(parseType(keyType), parseType(valueType), valueContainsNull = true)
      case other =>
        throw new IllegalArgumentException(s"Unknown type in schema override: $other")
    }
  }

  /** Split a map type's inner specification (e.g., "string,long") into key and value type strings.
    * Handles nested generics by tracking angle bracket depth.
    */
  private def splitMapTypes(inner: String): (String, String) = {
    var depth = 0
    for (i <- inner.indices)
      inner(i) match {
        case '<' => depth += 1
        case '>' => depth -= 1
        case ',' if depth == 0 =>
          return (inner.substring(0, i).trim, inner.substring(i + 1).trim)
        case _ =>
      }
    throw new IllegalArgumentException(
      s"Invalid map type specification: 'map<$inner>'. Expected format: map<keyType,valueType>"
    )
  }
}
