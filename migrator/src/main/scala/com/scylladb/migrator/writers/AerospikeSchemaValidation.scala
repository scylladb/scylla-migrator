package com.scylladb.migrator.writers

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.{ DataType => DriverDataType, DataTypes }
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.{ types => cql }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.scylladb.migrator.config.AerospikeInvalidRecordHandling
import scala.collection.immutable.ArraySeq

/** The connector accepts coercions that can discard values. Check Aerospike writes before those
  * coercions run, including nested members and the record identity.
  */
private[writers] object AerospikeSchemaValidation {
  final case class Column(
    name: String,
    dataType: cql.ColumnType[_],
    primaryKey: Boolean,
    singlePartitionKey: Boolean = false,
    partitionKey: Boolean = false
  ) extends Serializable {
    @transient lazy val driverDataType: DriverDataType = driverType(dataType)
    @transient lazy val keyConverter: cql.TypeConverter[_ <: AnyRef] =
      cql.ColumnType.converterToCassandra(driverDataType)
  }

  // Reconstruct only the already-supported target types. The declared type lets the driver encode
  // nested collections even when their first member is empty, without inferring types from values.
  private def driverType(target: cql.ColumnType[_]): DriverDataType = target match {
    case cql.TextType | cql.VarCharType => DataTypes.TEXT
    case cql.AsciiType                  => DataTypes.ASCII
    case cql.BooleanType                => DataTypes.BOOLEAN
    case cql.BlobType                   => DataTypes.BLOB
    case cql.TinyIntType                => DataTypes.TINYINT
    case cql.SmallIntType               => DataTypes.SMALLINT
    case cql.IntType                    => DataTypes.INT
    case cql.BigIntType                 => DataTypes.BIGINT
    case cql.FloatType                  => DataTypes.FLOAT
    case cql.DoubleType                 => DataTypes.DOUBLE
    case cql.VarIntType                 => DataTypes.VARINT
    case cql.DecimalType                => DataTypes.DECIMAL
    case t: cql.ListType[_]             => DataTypes.listOf(driverType(t.elemType), t.isFrozen)
    case t: cql.MapType[_, _] =>
      DataTypes.mapOf(driverType(t.keyType), driverType(t.valueType), t.isFrozen)
    case t =>
      throw new IllegalArgumentException(s"Unsupported Aerospike target type: ${t.cqlTypeName}")
  }

  def prepare(
    sourceSchema: StructType,
    renamedSchema: StructType,
    keyColumn: String,
    table: TableDef
  ): Vector[Column] = {
    val identityIndex = sourceSchema.fieldIndex(keyColumn)
    val identityName = renamedSchema(identityIndex).name
    val pkNames = table.primaryKey.map(_.columnName).toSet
    val partitionKeyNames = table.partitionKey.map(_.columnName).toSet
    val singlePartitionKeyName =
      Option.when(table.partitionKey.size == 1)(table.partitionKey.head.columnName)
    require(
      pkNames.contains(identityName),
      s"Aerospike record identity column '$identityName' must be part of the target primary key " +
        "to prevent distinct source records from overwriting each other."
    )
    val missingKeys = pkNames -- renamedSchema.fieldNames.toSet
    require(
      missingKeys.isEmpty,
      s"Aerospike source is missing target primary key column(s): ${missingKeys.toSeq.sorted.mkString(", ")}"
    )
    renamedSchema.fields.zipWithIndex.map { case (field, index) =>
      require(
        !Scylla.InternalColumns.contains(field.name),
        s"Aerospike column '${field.name}' is reserved by the Scylla connector; " +
          "configure a rename to a different target column."
      )
      val targetColumn = table.columnByName.getOrElse(
        field.name,
        throw new IllegalArgumentException(
          s"Aerospike column '${field.name}' is absent from the target table; configure a rename " +
            "or an explicit source projection."
        )
      )
      val targetType = targetColumn.columnType
      require(
        !targetColumn.isStatic || table.partitionKey.exists(_.columnName == identityName),
        s"Aerospike column '${field.name}' maps to a static column shared by multiple records; " +
          s"the record identity '$identityName' must be part of the partition key to preserve static values."
      )
      if (index == identityIndex)
        require(
          sameIdentityType(field.dataType, targetType),
          s"Aerospike record identity column '${field.name}' has type ${field.dataType.simpleString} " +
            s"but the target is ${targetType.cqlTypeName}; keyMode requires a matching target type."
        )
      validateTypes(field.dataType, targetType, field.name)
      Column(
        field.name,
        targetType,
        pkNames.contains(field.name),
        singlePartitionKeyName.contains(field.name),
        partitionKeyNames.contains(field.name)
      )
    }.toVector
  }

  private def sameIdentityType(source: DataType, target: cql.ColumnType[_]): Boolean =
    (source, target) match {
      case (StringType, cql.TextType | cql.VarCharType) => true
      case (LongType, cql.BigIntType)                   => true
      case (BinaryType, cql.BlobType)                   => true
      case _                                            => false
    }

  private def numericSource(source: DataType): Boolean = source match {
    case LongType | IntegerType | DoubleType => true
    case _                                   => false
  }

  private def numericTarget(target: cql.ColumnType[_]): Boolean = target match {
    case cql.TinyIntType | cql.SmallIntType | cql.IntType | cql.BigIntType | cql.FloatType |
        cql.DoubleType | cql.VarIntType | cql.DecimalType =>
      true
    case _ => false
  }

  private[writers] def validateTypes(
    source: DataType,
    target: cql.ColumnType[_],
    path: String
  ): Unit = (source, target) match {
    case (StringType, cql.TextType | cql.VarCharType | cql.AsciiType) =>
    case (BooleanType, cql.BooleanType)                               =>
    case (BinaryType, cql.BlobType)                                   =>
    case (s, t) if numericSource(s) && numericTarget(t)               =>
    case (s, cql.TextType | cql.VarCharType) if numericSource(s)      =>
    case (ArrayType(element, _), t: cql.ListType[_]) =>
      validateTypes(element, t.elemType, s"$path[]")
    case (MapType(key, value, _), t: cql.MapType[_, _]) =>
      validateTypes(key, t.keyType, s"$path<key>")
      validateTypes(value, t.valueType, s"$path<value>")
    case _ =>
      throw new IllegalArgumentException(
        s"Aerospike column '$path' cannot be migrated from ${source.simpleString} to " +
          s"${target.cqlTypeName} without changing its representation; use a compatible target " +
          "type or an explicit source schema. Lists require CQL lists to preserve order and duplicates."
      )
  }

  private[writers] final class InvalidValue(message: String, cause: Throwable = null)
      extends IllegalArgumentException(message, cause)

  def validateRow(
    row: Row,
    columns: Vector[Column],
    handling: AerospikeInvalidRecordHandling = AerospikeInvalidRecordHandling.Skip,
    onInvalid: String => Unit = _ => ()
  ): Option[Row] = {
    var replacement: Array[Any] = null
    var index = 0
    var partitionKeySize = 0L
    var clusteringKeySize = 0L
    while (index < columns.length) {
      val column = columns(index)
      val value = row.get(index)
      try {
        if (value == null && column.primaryKey)
          fail(column.name, "the target primary key value is null")
        if (column.singlePartitionKey && emptyKeyValue(value))
          fail(column.name, "the single-column target partition key is empty")
        validateValue(value, column.dataType, column.name)
        if (column.primaryKey) {
          val converted = column.keyConverter.convert(value)
          val encoded =
            CodecRegistry.DEFAULT
              .codecFor[AnyRef](column.driverDataType)
              .encode(converted, ProtocolVersion.DEFAULT)
          // Scylla's internal key representation prefixes every component (even a single one)
          // with two length bytes. Driver routing-key framing uses a different representation.
          val componentSize = encoded.remaining().toLong + 2L
          if (column.partitionKey) {
            partitionKeySize += componentSize
            if (partitionKeySize > 65535L)
              fail(column.name, "the encoded target partition key exceeds 65535 bytes")
          } else {
            clusteringKeySize += componentSize
            if (clusteringKeySize > 65535L)
              fail(column.name, "the encoded target clustering key exceeds 65535 bytes")
          }
        }
      } catch {
        case error: InvalidValue =>
          onInvalid(error.getMessage)
          if (column.primaryKey || handling == AerospikeInvalidRecordHandling.Skip) return None
          if (replacement == null) replacement = row.toSeq.toArray
          replacement(index)                   = null
      }
      index += 1
    }
    Some(if (replacement == null) row else Row.fromSeq(ArraySeq.unsafeWrapArray(replacement)))
  }

  private def emptyKeyValue(value: Any): Boolean = value match {
    case text: String                => text.isEmpty
    case bytes: Array[Byte]          => bytes.isEmpty
    case buffer: java.nio.ByteBuffer => !buffer.hasRemaining
    case _                           => false
  }

  private def fail(path: String, reason: String): Nothing =
    throw new InvalidValue(s"Cannot migrate Aerospike column '$path': $reason")

  /** Compare the actual connector result to the input, so connector numeric casts cannot silently
    * overflow, round, truncate, or collapse a negative zero.
    */
  private def exactNumber(number: Number): Option[java.math.BigDecimal] = number match {
    case n: java.lang.Double if !n.isNaN && !n.isInfinite =>
      Some(new java.math.BigDecimal(n.doubleValue()))
    case n: java.lang.Float if !n.isNaN && !n.isInfinite =>
      Some(new java.math.BigDecimal(n.doubleValue()))
    case _: java.lang.Double | _: java.lang.Float => None
    case n: java.math.BigDecimal                  => Some(n)
    case n: java.math.BigInteger                  => Some(new java.math.BigDecimal(n))
    case n => Some(java.math.BigDecimal.valueOf(n.longValue()))
  }

  private def negativeZero(number: Number): Boolean = number match {
    case n: java.lang.Double => java.lang.Double.doubleToRawLongBits(n) == Long.MinValue
    case n: java.lang.Float  => java.lang.Float.floatToRawIntBits(n) == Int.MinValue
    case _                   => false
  }

  private[writers] def validateValue(
    value: Any,
    target: cql.ColumnType[_],
    path: String
  ): Unit = {
    if (value == null) return
    (value, target) match {
      case (_: java.lang.Long, cql.BigIntType) | (_: java.lang.Double, cql.DoubleType) |
          (_: java.lang.Integer, cql.IntType | cql.BigIntType) =>
      case (number: Number, t) if numericTarget(t) =>
        val converted =
          try t.converterToCassandra.convert(number).asInstanceOf[Number]
          catch {
            case error @ (_: cql.TypeConversionException | _: NumberFormatException) =>
              throw new InvalidValue(
                s"Cannot migrate Aerospike column '$path': numeric value cannot be represented as ${t.cqlTypeName}",
                error
              )
          }
        val exact = (exactNumber(number), exactNumber(converted)) match {
          case (Some(before), Some(after)) => before.compareTo(after) == 0
          case (None, None) =>
            java.lang.Double.compare(number.doubleValue(), converted.doubleValue()) == 0
          case _ => false
        }
        if (!exact || negativeZero(number) != negativeZero(converted))
          fail(path, s"numeric conversion to ${t.cqlTypeName} would change the value")
      case (text: String, cql.AsciiType) =>
        if (!text.forall(_ <= 127)) fail(path, "the value contains non-ASCII characters")
      case (values: scala.collection.Seq[_], t: cql.ListType[_]) =>
        if (values.isEmpty && !t.isFrozen)
          fail(
            path,
            "an empty collection becomes null in a non-frozen CQL column; use frozen<list<...>>"
          )
        values.zipWithIndex.foreach { case (element, index) =>
          val memberPath = s"$path[$index]"
          if (element == null) fail(memberPath, "CQL collections cannot contain null members")
          validateValue(element, t.elemType, memberPath)
        }
      case (values: scala.collection.Map[_, _], t: cql.MapType[_, _]) =>
        if (values.isEmpty && !t.isFrozen)
          fail(
            path,
            "an empty collection becomes null in a non-frozen CQL column; use frozen<map<...>>"
          )
        val keys = scala.collection.mutable.HashSet.empty[Any]
        values.foreach { case (key, element) =>
          if (key == null || element == null)
            fail(path, "CQL maps cannot contain null keys or values")
          validateValue(key, t.keyType, s"$path<key>")
          validateValue(element, t.valueType, s"$path<value>")
          if (!keys.add(t.keyType.converterToCassandra.convert(key)))
            fail(path, "distinct map keys become the same target key after conversion")
        }
      case _ =>
    }
  }
}
