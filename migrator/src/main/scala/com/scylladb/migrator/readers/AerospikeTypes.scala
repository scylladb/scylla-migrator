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
    *
    * Aerospike is schema-less, so a bin's runtime type can differ from the type inferred during
    * sampling. Whatever the runtime-type rules produce is therefore checked against `expectedType`:
    * a mismatch is rendered as text for string columns, and otherwise replaced by null (with a
    * rate-limited warning) so a single unsampled value cannot fail the whole task with a
    * ClassCastException inside Spark's row encoder.
    */
  private[migrator] def convertValue(value: Any, expectedType: DataType): Any = {
    val converted = convertByRuntimeType(value, expectedType)
    if (converted == null || isCompatible(converted, expectedType)) converted
    else if (expectedType == StringType) converted.toString
    else {
      warnIncompatible(value, expectedType)
      null
    }
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

  /** Log `message` on the first occurrence of `counterKey` and every `WarnInterval` after that,
    * passing the running count so the operator can gauge how widespread the problem is.
    */
  private def warnRateLimited(counterKey: String)(message: Long => String): Unit = {
    val counter = warnedTypeCounts.computeIfAbsent(
      counterKey,
      _ => new java.util.concurrent.atomic.AtomicLong(0)
    )
    val count = counter.incrementAndGet()
    if (count == 1 || count % WarnInterval == 0) log.warn(message(count))
  }

  private def typeNameOf(value: Any): String =
    if (value == null) "null" else value.getClass.getName

  /** Rate-limited warning for values that cannot be represented as the inferred column type. */
  private def warnIncompatible(value: Any, expectedType: DataType): Unit = {
    val typeName = typeNameOf(value)
    warnRateLimited(s"incompatible:$typeName->$expectedType") { count =>
      s"convertValue: a value of type $typeName is not representable as $expectedType, " +
        s"writing null ($count occurrences so far). The column type came from the schema " +
        "sample; provide an explicit 'schema' override or raise 'schemaSampleSize' if this " +
        "is unexpected."
    }
  }

  /** Rate-limited warning for map entries dropped because their key could not be converted. */
  private def warnDroppedMapKey(key: Any, keyType: DataType): Unit = {
    val typeName = typeNameOf(key)
    warnRateLimited(s"mapkey:$typeName->$keyType") { count =>
      s"convertValue: dropping a map entry whose key of type $typeName is not representable " +
        s"as the inferred key type $keyType ($count entries dropped so far). Spark map keys " +
        "cannot be null. Provide an explicit 'schema' override or raise 'schemaSampleSize' if " +
        "this is unexpected."
    }
  }

  private def convertByRuntimeType(value: Any, expectedType: DataType): Any = value match {
    case null => null
    // Collection types first (most specific)
    case v: java.util.List[_] =>
      expectedType match {
        case ArrayType(et, _) =>
          v.asScala.map(e => convertValue(e, et)).toSeq
        case StringType =>
          // Serialize collection to string for an explicit StringType column
          v.toString
        case _ =>
          // Collection encountered where a scalar (non-String) column was inferred from the
          // sample. Return null rather than a String to avoid a ClassCastException at encoding.
          null
      }
    case v: java.util.Map[_, _] =>
      expectedType match {
        case MapType(k, vt, _) =>
          // A key that fails conversion cannot be written as null — Spark map keys are
          // non-nullable — and two such keys would silently collapse into a single entry under
          // `.toMap`. Drop those entries individually instead, and say so.
          v.asScala.iterator.flatMap { case (mk, mv) =>
            val convertedKey = convertValue(mk, k)
            if (convertedKey == null) {
              warnDroppedMapKey(mk, k)
              None
            } else Some(convertedKey -> convertValue(mv, vt))
          }.toMap
        case StringType =>
          // Serialize collection to string for an explicit StringType column
          v.toString
        case _ =>
          null
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
    // Catch-all: an unrecognized type is only representable as text. Warn on first occurrence
    // and then periodically (every 10,000 occurrences) per type to surface ongoing degradation.
    // For non-string columns the value is returned unchanged so the compatibility guard in
    // `convertValue` rejects it with a single, more specific warning.
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
    keyType match {
      case StringType =>
        if (key.userKey != null) key.userKey.toString else hexFormat.formatHex(key.digest)
      case BinaryType =>
        // Digest is always available as raw bytes; user keys written as blobs arrive as byte[].
        // A user key of any other runtime type is not representable here, so it gets the same
        // compatibility check as the other typed branches rather than reaching Spark's encoder.
        if (key.userKey == null) key.digest
        else requireCompatibleKey(key.userKey.getObject, keyType)
      case _ =>
        // A non-String/Binary key type was inferred from the sample. Neither a digest-only
        // record nor a key of a different runtime type is representable as that type, and
        // aero_key is non-nullable, so fail with an actionable message rather than emitting a
        // row the Spark encoder will reject.
        if (key.userKey == null)
          throw new IllegalStateException(
            s"Digest-only record (no user key) encountered, but the discovered key type is " +
              s"$keyType. Store keys with sendKey=true, or set an explicit 'schema' so 'aero_key' " +
              "is a string and the digest fallback is representable."
          )
        else requireCompatibleKey(key.userKey.getObject, keyType)
    }

  /** Return the user key when it matches the discovered key type, otherwise fail with guidance.
    *
    * `aero_key` is non-nullable, so a mismatch cannot be dropped the way a bin value can.
    */
  private def requireCompatibleKey(userKey: Any, keyType: DataType): Any =
    if (isCompatible(userKey, keyType)) userKey
    else
      throw new IllegalStateException(
        s"Aerospike record key of type ${typeNameOf(userKey)} does not match the discovered " +
          s"key type $keyType. This set mixes key types that the schema sample did not observe. " +
          "Set an explicit 'schema' so 'aero_key' is a string, or raise 'schemaSampleSize' so " +
          "the mixed types are detected during discovery."
      )

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
