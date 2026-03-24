package com.scylladb.migrator.readers

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.types._

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** Type inference, conversion, and schema helpers for the Aerospike reader. */
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

  // The Aerospike Java client returns: Long, Double, String, byte[], List, Map.
  // Values from CDTs (lists/maps) may also arrive as Integer or Float.
  // Boolean may appear in CDTs or future client versions.
  // GeoJSON values arrive as String from the client; HLL values arrive as byte[].
  private[migrator] def inferSparkType(value: Any): DataType = value match {
    case _: java.lang.Long    => LongType
    case _: java.lang.Integer => LongType
    case _: java.lang.Double  => DoubleType
    case _: java.lang.Float   => DoubleType
    case _: java.lang.Boolean => BooleanType
    case _: String            => StringType // also covers GeoJSON (arrives as JSON string)
    case _: Array[Byte]       => BinaryType // also covers HLL (arrives as raw bytes)
    case v: java.util.List[_] => ArrayType(inferCommonType(v.asScala), containsNull = true)
    case v: java.util.Map[_, _] =>
      MapType(
        inferCommonType(v.asScala.keys),
        inferCommonType(v.asScala.values),
        valueContainsNull = true
      )
    case other =>
      log.debug(
        s"inferSparkType: unrecognized type ${other.getClass.getName}, defaulting to StringType"
      )
      StringType
  }

  /** Infer a single common type from a collection of values. Falls back to StringType on mixed
    * types. Short-circuits after finding 2 distinct types to avoid scanning large CDTs.
    */
  private def inferCommonType(values: Iterable[_]): DataType = {
    var first: DataType = null
    val iter = values.iterator
    while (iter.hasNext) {
      val v = iter.next()
      if (v != null) {
        val t = inferSparkType(v)
        if (first == null) first = t
        else if (first != t) return StringType // heterogeneous -> short-circuit
      }
    }
    if (first == null) StringType // empty -> String
    else first
  }

  /** Merge two types discovered across different records. Supports numeric widening (Long ->
    * Double) and recursive collection merging. Falls back to StringType for incompatible types.
    */
  private[migrator] def mergeTypes(a: DataType, b: DataType): DataType = (a, b) match {
    case (x, y) if x == y                                => x
    case (LongType, DoubleType) | (DoubleType, LongType) => DoubleType
    case (ArrayType(et1, n1), ArrayType(et2, n2)) =>
      ArrayType(mergeTypes(et1, et2), n1 || n2)
    case (MapType(kt1, vt1, n1), MapType(kt2, vt2, n2)) =>
      MapType(mergeTypes(kt1, kt2), mergeTypes(vt1, vt2), n1 || n2)
    case _ => StringType
  }

  /** Convert an Aerospike value to a Spark-compatible value, coercing to match the expected type.
    */
  private[migrator] def convertValue(value: Any, expectedType: DataType): Any = value match {
    case null => null
    // Collection types first (most specific)
    case v: java.util.List[_] =>
      expectedType match {
        case ArrayType(et, _) =>
          v.asScala.map(e => convertValue(e, et)).toSeq
        case _ =>
          // Serialize collection to string for non-array target types (e.g., StringType)
          v.toString
      }
    case v: java.util.Map[_, _] =>
      expectedType match {
        case MapType(k, vt, _) =>
          v.asScala.map { case (mk, mv) => convertValue(mk, k) -> convertValue(mv, vt) }.toMap
        case _ =>
          // Serialize collection to string for non-map target types (e.g., StringType)
          v.toString
      }
    // Primitive types — handle StringType and DoubleType coercion inline
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
        case DoubleType => java.lang.Double.valueOf(v.doubleValue())
        case _          => v
      }
    case v: java.lang.Double =>
      if (expectedType == StringType) v.toString else v
    case v: String => v
    case v: java.lang.Boolean =>
      if (expectedType == BooleanType) v else v.toString
    // Catch-all: coerce anything else to String. Warn on first occurrence and then
    // periodically (every 10,000 occurrences) per type to surface ongoing degradation.
    case v =>
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
  }

  /** Clear rate-limited warning state. Intended for tests to prevent state leaking across suites.
    */
  private[migrator] def reset(): Unit = warnedTypeCounts.clear()

  /** Fail fast on the driver if JDK < 17, before submitting any Spark tasks. */
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

  /** Extract the key value for the aero_key column, typed according to the schema */
  private[migrator] def extractKey(key: com.aerospike.client.Key, keyType: DataType): Any =
    if (key.userKey != null) {
      keyType match {
        case StringType => key.userKey.toString
        case _          => key.userKey.getObject
      }
    } else hexFormat.formatHex(key.digest)

  /** Parse a user-provided type name into a Spark DataType. Supports scalar types (string, long,
    * double, binary) and collection types (list<T>, map<K,V>). Collection types can be nested,
    * e.g., list<map<string,long>>.
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
