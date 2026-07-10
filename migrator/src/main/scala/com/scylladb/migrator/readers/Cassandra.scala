package com.scylladb.migrator.readers

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{ CassandraConnector, ColumnDef, Schema, TableDef }
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.types.{
  AsciiType,
  BigIntType,
  CassandraOption,
  ColumnType,
  IntType,
  ListType => CqlListType,
  MapType => CqlMapType,
  SetType => CqlSetType,
  SmallIntType,
  TextType,
  TinyIntType,
  VarCharType
}
import com.scylladb.migrator.Connectors
import com.scylladb.migrator.config.{ CopyType, SourceSettings }
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra.{ CassandraSQLRow, DataTypeConverter }
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  IntegerType,
  LongType,
  MapType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.unsafe.types.UTF8String
import com.scylladb.migrator.ConsistencyLevelUtils
import com.scylladb.migrator.scylla.{ CollectionAppendWrite, SourceDataFrame }

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import java.nio.charset.StandardCharsets

object Cassandra {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Cassandra")

  /** Above this element count, a single non-frozen collection cell is logged as a potential
    * memory/throughput hot spot: per-element TTL/WRITETIME preservation may expand it into up to
    * this many separate collection-append updates (one per distinct TTL/WRITETIME group).
    */
  private val LargeCollectionWarnThreshold = 100000

  case class Selection(
    columnRefs: List[ColumnRef],
    schema: StructType,
    timestampColumns: Option[TimestampColumns]
  )

  /** Regular columns that are non-frozen (multi-cell) collections. These keep per-element
    * TTL/WRITETIME (one metadata value per element), so `TTL()`/`WRITETIME()` on them return a list
    * aligned with the collection's elements rather than a scalar.
    */
  def nonFrozenCollectionColumns(tableDef: TableDef): Seq[ColumnDef] =
    tableDef.regularColumns.filter(c => c.columnType.isCollection && c.columnType.isMultiCell)

  private def quoteCqlIdentifier(id: String): String =
    "\"" + id.replace("\"", "\"\"") + "\""

  /** Fail fast if the source server cannot return per-element `WRITETIME()`/`TTL()` for a
    * non-frozen collection column. We only PREPARE the statement (no execution, no data read); on
    * servers older than Cassandra 5.0 / older ScyllaDB this fails at semantic validation, letting
    * us surface a clear, actionable error before launching the distributed read.
    */
  private def assertNonFrozenCollectionMetadataReadable(
    connector: CassandraConnector,
    keyspace: String,
    table: String,
    nonFrozen: Seq[ColumnDef]
  ): Unit =
    nonFrozen.headOption.foreach { col =>
      val c = quoteCqlIdentifier(col.columnName)
      val cql =
        s"SELECT WRITETIME($c), TTL($c) FROM " +
          s"${quoteCqlIdentifier(keyspace)}.${quoteCqlIdentifier(table)} LIMIT 1"
      try connector.withSessionDo(_.prepare(cql))
      catch {
        case NonFatal(e) =>
          throw new IllegalStateException(
            s"preserveCollectionTimestamps is enabled, but the source cannot read per-element " +
              s"WRITETIME()/TTL() on non-frozen collection column '${col.columnName}'. This " +
              s"requires Cassandra 5.0+ or a modern ScyllaDB. Underlying error: ${e.getMessage}",
            e
          )
      }
    }

  /** Unsigned lexicographic ordering over byte arrays, matching Cassandra's `UTF8Type`/`AsciiType`
    * comparator for text map keys.
    */
  private val unsignedBytesOrdering: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = {
      val n = math.min(a.length, b.length)
      var i = 0
      while (i < n) {
        val ai = a(i) & 0xff
        val bi = b(i) & 0xff
        if (ai != bi) return ai - bi
        i += 1
      }
      a.length - b.length
    }
  }

  /** Ordering over runtime map-key values that reproduces Cassandra's on-disk key order for the
    * supported key types, so a decoded map (whose iteration order Spark/Catalyst does not preserve)
    * can be re-aligned with the element-order `WRITETIME`/`TTL` lists. Returns `None` for key types
    * whose Cassandra comparator we cannot faithfully reproduce here.
    */
  def mapKeyOrdering(keyType: ColumnType[_]): Option[Ordering[Any]] = keyType match {
    case TextType | AsciiType | VarCharType =>
      Some(
        Ordering.by((k: Any) => k.toString.getBytes(StandardCharsets.UTF_8))(unsignedBytesOrdering)
      )
    case IntType | BigIntType | SmallIntType | TinyIntType =>
      Some(Ordering.by((k: Any) => k.asInstanceOf[Number].longValue()))
    case _ => None
  }

  /** Describe why a non-frozen collection column cannot have its per-element timestamps preserved,
    * or `None` if it is supported. Non-frozen lists are unsupported (list cells are keyed by
    * generated timeuuids, so append cannot reproduce element identity), and maps are supported only
    * for key types with a reproducible ordering (see [[mapKeyOrdering]]).
    */
  private def unsupportedCollectionReason(column: ColumnDef): Option[String] =
    column.columnType match {
      case _: CqlListType[_] =>
        Some(s"'${column.columnName}' (non-frozen list)")
      case m: CqlMapType[_, _] if mapKeyOrdering(m.keyType).isEmpty =>
        Some(s"'${column.columnName}' (non-frozen map with unsupported key type ${m.keyType})")
      case s: CqlSetType[_] if mapKeyOrdering(s.elemType).isEmpty =>
        Some(s"'${column.columnName}' (non-frozen set with unsupported element type ${s.elemType})")
      case _ => None
    }

  /** The ordering used to re-align a non-frozen collection's decoded elements with its
    * element-order `WRITETIME`/`TTL` lists: map keys for maps, element values for sets (Cassandra
    * stores/returns both in that sorted order). `None` for unsupported element/key types.
    */
  private def collectionElementOrdering(columnType: ColumnType[_]): Option[Ordering[Any]] =
    columnType match {
      case m: CqlMapType[_, _] => mapKeyOrdering(m.keyType)
      case s: CqlSetType[_]    => mapKeyOrdering(s.elemType)
      case _                   => None
    }

  def determineCopyType(
    tableDef: TableDef,
    preserveTimesRequest: Boolean,
    preserveCollectionTimesRequest: Boolean = false
  ): Either[Throwable, CopyType] = {
    // Frozen collections are stored as a single cell, so CQL `TTL()`/`WRITETIME()` return one
    // scalar value per column and flow through the existing scalar preservation path unchanged.
    // Non-frozen (multi-cell) collections keep per-element TTL/writetime. They are rejected unless
    // the opt-in `preserveCollectionTimestamps` is enabled, in which case supported ones (sets and
    // maps with an orderable key type) are read as element-aligned lists and re-applied per element
    // via collection-append writes.
    val nonFrozen = nonFrozenCollectionColumns(tableDef)
    if (nonFrozen.nonEmpty && preserveTimesRequest && !preserveCollectionTimesRequest)
      Left(
        new Exception(
          "TTL/Writetime preservation is unsupported for tables with non-frozen (multi-cell) " +
            "collection types (lists, maps, sets). Freeze the collection columns, enable the config " +
            "option 'preserveCollectionTimestamps' to preserve per-element TTL/WRITETIME (supported " +
            "for sets and maps on Cassandra 5.0+/modern ScyllaDB sources), or set the config option " +
            "'preserveTimestamps' to false to continue."
        )
      )
    else if (nonFrozen.nonEmpty && preserveTimesRequest && preserveCollectionTimesRequest) {
      val unsupported = nonFrozen.flatMap(unsupportedCollectionReason)
      if (unsupported.nonEmpty)
        Left(
          new Exception(
            "Per-element TTL/Writetime preservation ('preserveCollectionTimestamps') is unsupported " +
              s"for these columns: ${unsupported.mkString(", ")}. Freeze them, or set " +
              "'preserveCollectionTimestamps' to false to continue."
          )
        )
      else Right(CopyType.WithTimestampPreservation)
    } else if (preserveTimesRequest && tableDef.regularColumns.nonEmpty)
      Right(CopyType.WithTimestampPreservation)
    else if (preserveTimesRequest && tableDef.regularColumns.isEmpty) {
      log.warn("No regular columns in the table - disabling timestamp preservation")
      Right(CopyType.NoTimestampPreservation)
    } else Right(CopyType.NoTimestampPreservation)
  }

  def createSelection(
    tableDef: TableDef,
    origSchema: StructType,
    preserveTimes: Boolean,
    preserveCollectionTimes: Boolean = false
  ): Either[Throwable, Selection] =
    determineCopyType(tableDef, preserveTimes, preserveCollectionTimes) map {
      case CopyType.WithTimestampPreservation =>
        // When per-element preservation is enabled, non-frozen collection columns select their
        // TTL()/WRITETIME() as element-aligned lists (ArrayType sidecars) instead of scalars.
        val perElementCollectionNames =
          if (preserveCollectionTimes) nonFrozenCollectionColumns(tableDef).map(_.columnName).toSet
          else Set.empty[String]

        val columnRefs =
          tableDef.partitionKey.map(_.ref) ++
            tableDef.clusteringColumns.map(_.ref) ++
            tableDef.regularColumns.flatMap { column =>
              val colName = column.columnName

              List(
                column.ref,
                colName.ttl as s"${colName}_ttl",
                colName.writeTime as s"${colName}_writetime"
              )
            }

        log.info("ColumnRefs generated for selection:")
        log.info(columnRefs.mkString("\n"))

        val schema = StructType(for {
          origField <- origSchema.fields
          isRegular = tableDef.regularColumns.exists(_.ref.columnName == origField.name)
          isPerElementCollection = perElementCollectionNames.contains(origField.name)
          ttlType       = if (isPerElementCollection) ArrayType(IntegerType) else IntegerType
          writetimeType = if (isPerElementCollection) ArrayType(LongType) else LongType
          field <- if (isRegular)
                     List(
                       origField,
                       StructField(s"${origField.name}_ttl", ttlType, true),
                       StructField(s"${origField.name}_writetime", writetimeType, true)
                     )
                   else List(origField)
        } yield field)

        log.info("Schema generated with TTLs and Writetimes:")
        schema.printTreeString()

        Selection(columnRefs.toList, schema, Some(TimestampColumns("ttl", "writetime")))

      case CopyType.NoTimestampPreservation =>
        // We're not using the `tableDef.allColumns` property here in order to generate
        // a schema that is consistent with the timestamp preservation case; the ordering
        // must be (partition keys, clustering keys, regular columns).
        val columnRefs = (tableDef.partitionKey.map(_.ref) ++
          tableDef.clusteringColumns.map(_.ref) ++
          tableDef.regularColumns.map(_.ref)).toList

        log.info("ColumnRefs generated for selection:")
        log.info(columnRefs.mkString("\n"))
        log.info("Schema generated:")
        origSchema.printTreeString()

        Selection(columnRefs, origSchema, None)
    }

  def explodeRow(
    row: Row,
    schema: StructType,
    primaryKeyOrdinals: Map[String, Int],
    regularKeyOrdinals: Map[String, (Int, Int, Int)]
  ) =
    if (regularKeyOrdinals.isEmpty) List(row)
    else {
      val rowTimestampsToFields =
        regularKeyOrdinals
          .map { case (fieldName, (ordinal, ttlOrdinal, writetimeOrdinal)) =>
            (
              fieldName,
              if (row.isNullAt(ordinal)) CassandraOption.Null
              else CassandraOption.Value(row.get(ordinal)),
              if (row.isNullAt(ttlOrdinal)) None
              else Some(row.getInt(ttlOrdinal)),
              if (row.isNullAt(writetimeOrdinal)) None
              else Some(row.getLong(writetimeOrdinal))
            )
          }
          .groupBy { case (fieldName, value, ttl, writetime) =>
            (ttl, writetime)
          }
          .view
          .mapValues(
            _.map { case (fieldName, value, _, _) =>
              fieldName -> value
            }.toMap
          )
          .toMap

      // This is an optimisation to avoid unnecessary inserts and tombstones:
      // If there are multiple rows to insert, remove the row containing NULLs
      // (since those will be "inserted" as a result of inserting the remaining rows)
      val timestampsToFields =
        if (rowTimestampsToFields.size > 1) rowTimestampsToFields.-((None, None))
        else rowTimestampsToFields

      timestampsToFields
        .map { case ((ttl, writetime), fields) =>
          val newValues = schema.fields.map { field =>
            primaryKeyOrdinals
              .get(field.name)
              .map { ord =>
                if (row.isNullAt(ord)) null
                else convertValue(row.get(ord))
              }
              .getOrElse(fields.getOrElse(field.name, CassandraOption.Unset))
          } ++ Seq(
            Integer.valueOf(ttl.getOrElse(0)),
            writetime.map(java.lang.Long.valueOf).getOrElse(CassandraOption.Unset)
          )

          Row(ArraySeq.unsafeWrapArray(newValues): _*)
        }
    }

  /** Explode a wide row into base rows for the timestamp-preservation write, EXCLUDING non-frozen
    * collection columns (whose per-element timestamps are handled by separate collection-append
    * passes). `baseSchema` must already omit those columns. When there are no base regular columns
    * (a table whose only regular columns are per-element collections), a single primary-key-only
    * row is emitted so the base write establishes the partition; the collection appends carry the
    * data.
    */
  def explodeBaseRow(
    row: Row,
    baseSchema: StructType,
    primaryKeyOrdinals: Map[String, Int],
    baseRegularKeyOrdinals: Map[String, (Int, Int, Int)]
  ): Iterable[Row] =
    if (baseRegularKeyOrdinals.nonEmpty)
      explodeRow(row, baseSchema, primaryKeyOrdinals, baseRegularKeyOrdinals)
    else {
      val newValues = baseSchema.fields.map { field =>
        primaryKeyOrdinals
          .get(field.name)
          .map(ord => if (row.isNullAt(ord)) null else convertValue(row.get(ord)))
          .getOrElse(CassandraOption.Unset)
      } ++ Seq(Integer.valueOf(0), CassandraOption.Unset)
      List(Row(ArraySeq.unsafeWrapArray(newValues): _*))
    }

  private def asNumberSeq(v: Any): IndexedSeq[Any] = v match {
    case s: scala.collection.Seq[Any] => s.toIndexedSeq
    case a: Array[_]                  => ArraySeq.unsafeWrapArray(a.asInstanceOf[Array[Any]])
    case _                            => IndexedSeq.empty
  }

  /** Build the per-element collection-append rows for one non-frozen collection column of a single
    * wide source row. Each emitted row is `[primary key values..., grouped collection value, ttl,
    * writetime]`. Elements are aligned with their element-order `TTL()`/`WRITETIME()` lists (sets
    * by natural array order, maps by re-sorting entries with `keyOrdering` to match Cassandra's
    * on-disk key order), then grouped by `(ttl, writetime)` so co-timestamped elements share one
    * append.
    */
  def collectionAppendRows(
    row: Row,
    pkOrdinals: Array[Int],
    colOrdinal: Int,
    ttlOrdinal: Int,
    writetimeOrdinal: Int,
    isMap: Boolean,
    elementOrdering: Option[Ordering[Any]]
  ): Seq[Row] = {
    if (row.isNullAt(colOrdinal)) return Nil

    val pkValues: Seq[Any] =
      pkOrdinals.toIndexedSeq.map(o => if (row.isNullAt(o)) null else row.get(o))

    val ttlSeq =
      if (row.isNullAt(ttlOrdinal)) IndexedSeq.empty else asNumberSeq(row.get(ttlOrdinal))
    val wtSeq =
      if (row.isNullAt(writetimeOrdinal)) IndexedSeq.empty
      else asNumberSeq(row.get(writetimeOrdinal))

    def ttlAt(i: Int): Int =
      if (i < ttlSeq.length && ttlSeq(i) != null) ttlSeq(i).asInstanceOf[Number].intValue() else 0
    def wtAt(i: Int): Option[Long] =
      if (i < wtSeq.length && wtSeq(i) != null) Some(wtSeq(i).asInstanceOf[Number].longValue())
      else None

    def rowsFrom(elements: IndexedSeq[Any], rebuild: Seq[Any] => Any): Seq[Row] = {
      require(
        wtSeq.length == elements.length && (ttlSeq.isEmpty || ttlSeq.length == elements.length),
        s"Collection metadata length mismatch for a per-element collection column: " +
          s"${elements.length} elements but ${wtSeq.length} writetimes / ${ttlSeq.length} ttls. " +
          "Element<->timestamp alignment cannot be guaranteed; aborting to avoid silent data loss."
      )
      if (elements.length > LargeCollectionWarnThreshold)
        log.warn(
          s"Non-frozen collection cell has ${elements.length} elements; per-element TTL/WRITETIME " +
            "preservation may expand it into up to that many separate collection-append updates " +
            "(one per distinct TTL/WRITETIME group). Very large collections can cause high " +
            "executor memory/GC pressure and slow writes."
        )
      elements.indices
        .groupBy(i => (ttlAt(i), wtAt(i)))
        .toSeq
        .flatMap { case ((ttl, wtOpt), indices) =>
          wtOpt.map { wt =>
            val collectionValue = rebuild(indices.map(elements))
            Row.fromSeq(
              pkValues ++ Seq(
                collectionValue,
                Integer.valueOf(ttl),
                java.lang.Long.valueOf(wt)
              )
            )
          }
        }
    }

    // Both the connector's decoded set and map lose their server (sorted) order, while the
    // WRITETIME()/TTL() lists arrive in that sorted order. Re-sort the elements/entries with the
    // Cassandra-compatible ordering so element[i] pairs with its own metadata[i].
    val ordering = elementOrdering.getOrElse(
      throw new IllegalStateException(
        "Missing element ordering for a per-element collection column; this should have been " +
          "rejected earlier"
      )
    )

    if (isMap) {
      val m = row.get(colOrdinal).asInstanceOf[scala.collection.Map[Any, Any]]
      if (m.isEmpty) Nil
      else {
        val entries = m.toIndexedSeq.sortBy(_._1)(ordering)
        rowsFrom(
          entries.asInstanceOf[IndexedSeq[Any]],
          parts => parts.map(_.asInstanceOf[(Any, Any)]).toMap
        )
      }
    } else {
      val elems = row.get(colOrdinal).asInstanceOf[scala.collection.Seq[Any]].toIndexedSeq
      if (elems.isEmpty) Nil
      else rowsFrom(elems.sorted(ordering), parts => parts.toIndexedSeq)
    }
  }

  /** Build one [[CollectionAppendWrite]] per supported non-frozen collection column, reading the
    * per-element metadata from the wide `rawDataframe` and aligning it with each element.
    */
  def buildCollectionAppendWrites(
    spark: SparkSession,
    rawDataframe: DataFrame,
    tableDef: TableDef,
    origSchema: StructType,
    primaryKeyOrdinals: Map[String, Int],
    regularKeyOrdinals: Map[String, (Int, Int, Int)],
    perElementNames: Set[String]
  ): Seq[CollectionAppendWrite] = {
    val pkOrder =
      (tableDef.partitionKey ++ tableDef.clusteringColumns)
        .map(_.columnName)
        .filter(primaryKeyOrdinals.contains)
    val pkOrdinalArray = pkOrder.map(primaryKeyOrdinals).toArray
    val pkFields = pkOrder.map(name => origSchema(origSchema.fieldIndex(name)))

    nonFrozenCollectionColumns(tableDef)
      .filter(c => perElementNames.contains(c.columnName))
      .map { column =>
        val colName = column.columnName
        val (colOrd, ttlOrd, wtOrd) = regularKeyOrdinals(colName)
        val collectionField = origSchema(origSchema.fieldIndex(colName))
        val appendSchema = StructType(
          pkFields ++ Seq(
            collectionField,
            StructField("ttl", IntegerType, true),
            StructField("writetime", LongType, true)
          )
        )
        val isMap = column.columnType.isInstanceOf[CqlMapType[_, _]]
        val elementOrdering = collectionElementOrdering(column.columnType)
        val pkOrdinalsBroadcast = spark.sparkContext.broadcast(pkOrdinalArray)
        val rdd = rawDataframe.rdd.flatMap { row =>
          collectionAppendRows(
            row,
            pkOrdinalsBroadcast.value,
            colOrd,
            ttlOrd,
            wtOrd,
            isMap,
            elementOrdering
          )
        }
        CollectionAppendWrite(colName, rdd, appendSchema)
      }
  }

  /** Convert Cassandra-specific types to standard Spark types.
    *
    * Handles UTF8Strings, UDTValues, TupleValues, and collections thereof. These conversions are
    * done in the SourceRelation connector of the DataFrame API but need to be replicated here since
    * we use the RDD API.
    */
  val convertValue: Any => Any = {
    case x: UTF8String => x.toString
    case set: Set[_]   => set.map(convertValue)
    case list: List[_] => list.map(convertValue)
    case map: Map[_, _] =>
      map.map { case (k, v) =>
        convertValue(k) -> convertValue(v)
      }
    case ab: ArrayBuffer[_] => ab.map(convertValue)
    case udt: UDTValue      => Row.fromSeq(udt.columnValues.map(convertValue))
    case tuple: TupleValue  => Row.fromSeq(tuple.values.map(convertValue))
    case x                  => x
  }

  /** CQL `timestamp` is a 64-bit signed integer of milliseconds since epoch, so its range is
    * `[Long.MinValue, Long.MaxValue]` ms. Spark's `TimestampType` is encoded internally as
    * microseconds since epoch (also Long), so building a `TimestampType` column from a row whose
    * millis value lies outside `±(Long.MaxValue / 1000)` overflows in
    * `DateTimeUtils.millisToMicros` (`Math.multiplyExact(millis, 1000)`). To migrate the full CQL
    * range losslessly, replace `TimestampType` with `LongType` (epoch millis) in the source schema.
    * Recurses into UDT (`StructType`), list/set (`ArrayType`), and map (`MapType`).
    */
  def widenCqlTimestamps(dataType: DataType): DataType = dataType match {
    case TimestampType => LongType
    case s: StructType =>
      StructType(s.fields.map(f => f.copy(dataType = widenCqlTimestamps(f.dataType))))
    case a: ArrayType =>
      a.copy(elementType = widenCqlTimestamps(a.elementType))
    case m: MapType =>
      m.copy(keyType = widenCqlTimestamps(m.keyType), valueType = widenCqlTimestamps(m.valueType))
    case other => other
  }

  /** Recursively convert any `java.sql.Timestamp`/`java.util.Date`/`java.time.Instant` produced by
    * the Spark Cassandra connector to its epoch-millisecond `Long` value. Pairs with
    * [[widenCqlTimestamps]] so each row's runtime value matches the widened schema. The Scylla
    * writer (`saveToCassandra` via `SqlRowWriter`) accepts `Long` as millis for CQL `timestamp`
    * columns via the connector's `TypeConverter`, so the round-trip stays lossless.
    */
  val widenTimestampValue: Any => Any = {
    case t: java.sql.Timestamp => t.getTime
    case i: java.time.Instant  => i.toEpochMilli
    case d: java.util.Date     => d.getTime
    case row: Row              => Row.fromSeq(row.toSeq.map(widenTimestampValue))
    case set: Set[_]           => set.map(widenTimestampValue)
    case list: List[_]         => list.map(widenTimestampValue)
    case map: Map[_, _] =>
      map.map { case (k, v) =>
        widenTimestampValue(k) -> widenTimestampValue(v)
      }
    case ab: ArrayBuffer[_] => ab.map(widenTimestampValue)
    case x                  => x
  }

  def indexFields(
    currentFieldNames: List[String],
    origFieldNames: List[String],
    tableDef: TableDef
  ) = {
    val fieldIndices = currentFieldNames.zipWithIndex.toMap
    val primaryKeyIndices =
      (for {
        origFieldName <- origFieldNames
        if tableDef.primaryKey.exists(_.ref.columnName == origFieldName)
        index <- fieldIndices.get(origFieldName)
      } yield origFieldName -> index).toMap

    val regularKeyIndices =
      (for {
        origFieldName <- origFieldNames
        if tableDef.regularColumns.exists(_.ref.columnName == origFieldName)
        fieldIndex     <- fieldIndices.get(origFieldName)
        ttlIndex       <- fieldIndices.get(s"${origFieldName}_ttl")
        writetimeIndex <- fieldIndices.get(s"${origFieldName}_writetime")
      } yield origFieldName -> (fieldIndex, ttlIndex, writetimeIndex)).toMap

    (primaryKeyIndices, regularKeyIndices)
  }

  /** Infer primary key and regular column ordinals from the schema, based on the naming convention
    * used by `createSelection`: regular columns have companion `_ttl` and `_writetime` columns.
    */
  def indexFieldsFromSchema(
    schema: StructType
  ): (Map[String, Int], Map[String, (Int, Int, Int)]) = {
    val fieldNames = schema.fields.map(_.name).toList
    val fieldIndices = fieldNames.zipWithIndex.toMap

    val regularColumnNames = fieldNames.filter { name =>
      !name.endsWith("_ttl") && !name.endsWith("_writetime") &&
      fieldIndices.contains(s"${name}_ttl") && fieldIndices.contains(s"${name}_writetime")
    }

    val regularKeyIndices = regularColumnNames.map { name =>
      name -> (fieldIndices(name), fieldIndices(s"${name}_ttl"), fieldIndices(s"${name}_writetime"))
    }.toMap

    val metaColumnNames =
      regularColumnNames.flatMap(name => Set(s"${name}_ttl", s"${name}_writetime")).toSet
    val primaryKeyIndices = fieldNames
      .filter(name => !regularKeyIndices.contains(name) && !metaColumnNames.contains(name))
      .map(name => name -> fieldIndices(name))
      .toMap

    (primaryKeyIndices, regularKeyIndices)
  }

  /** Perform the row explosion on a DataFrame with per-column `_ttl`/`_writetime` columns.
    *
    * This is used when reading from Parquet files that contain per-column timestamp metadata, and
    * when repairing missing rows in the Scylla validator, to produce rows for writing to Scylla via
    * `saveToCassandra()` on an [[org.apache.spark.rdd.RDD]].
    *
    * Exploded rows keep [[CassandraOption]] on regular columns so [[CassandraOption.Null]]
    * (explicit CQL null) stays distinct from [[CassandraOption.Unset]] (column not in the current
    * TTL/writetime group). Spark 4 cannot represent that tri-state through a [[DataFrame]] row
    * encoder, so the write path uses [[writers.Scylla.writeRowRDD]] instead of
    * [[writers.Scylla.writeDataframe]].
    *
    * @return
    *   exploded rows, logical [[StructType]] for the write, and [[TimestampColumns]] for per-row
    *   TTL/writetime.
    */
  def explodeRowsFromPerColumnMeta(
    spark: SparkSession,
    df: DataFrame
  ): (RDD[Row], StructType, TimestampColumns) = {
    val (primaryKeyOrdinals, regularKeyOrdinals) = indexFieldsFromSchema(df.schema)

    val metaColumns =
      regularKeyOrdinals.keys.flatMap(name => Set(s"${name}_ttl", s"${name}_writetime")).toSet
    val origSchema = StructType(df.schema.fields.filterNot(f => metaColumns.contains(f.name)))

    val timestampColumns = TimestampColumns("ttl", "writetime")

    val broadcastPrimaryKeyOrdinals = spark.sparkContext.broadcast(primaryKeyOrdinals)
    val broadcastRegularKeyOrdinals = spark.sparkContext.broadcast(regularKeyOrdinals)
    val broadcastSchema = spark.sparkContext.broadcast(origSchema)
    val finalSchema = StructType(
      origSchema.fields ++
        Seq(StructField("ttl", IntegerType, true), StructField("writetime", LongType, true))
    )

    log.info("Schema after explosion from per-column metadata:")
    log.info(finalSchema.treeString)

    val explodedRdd = df.rdd.flatMap { row =>
      explodeRow(
        row,
        broadcastSchema.value,
        broadcastPrimaryKeyOrdinals.value,
        broadcastRegularKeyOrdinals.value
      )
    }

    (explodedRdd, finalSchema, timestampColumns)
  }

  /** @param skipExplosion
    *   when `true`, return the raw DataFrame with per-column `_ttl`/`_writetime` columns (standard
    *   Spark types). When `false` and timestamps are preserved, [[cassandraExplodedWrite]] carries
    *   exploded `RDD` of [[org.apache.spark.sql.Row]] for the write path while [[dataFrame]] stays
    *   the wide pre-explosion frame (Cassandra token metadata). Use `skipExplosion` true when
    *   writing to Parquet.
    */
  def readDataframe(
    spark: SparkSession,
    source: SourceSettings.Cassandra,
    preserveTimes: Boolean,
    tokenRangesToSkip: Set[(Token[_], Token[_])],
    skipExplosion: Boolean = false
  ): SourceDataFrame = {
    val connector = Connectors.sourceConnector(spark.sparkContext.getConf, source)
    val consistencyLevel = ConsistencyLevelUtils.parseConsistencyLevel(source.consistencyLevel)
    log.info(
      s"Using consistencyLevel [${consistencyLevel}] for SOURCE based on source config [${source.consistencyLevel}]"
    )

    val readConf = ReadConf
      .fromSparkConf(spark.sparkContext.getConf)
      .copy(
        splitCount       = source.splitCount,
        fetchSizeInRows  = source.fetchSize,
        consistencyLevel = consistencyLevel
      )

    val tableDef =
      connector.withSessionDo(Schema.tableFromCassandra(_, source.keyspace, source.table))
    log.info("TableDef retrieved for source:")
    log.info(tableDef)

    val origSchema = StructType(tableDef.columns.map { col =>
      val field = DataTypeConverter.toStructField(col)
      field.copy(dataType = widenCqlTimestamps(field.dataType))
    })
    log.info("Original schema loaded:")
    origSchema.printTreeString()

    val selection =
      createSelection(tableDef, origSchema, preserveTimes, source.preserveCollectionTimestamps)
        .fold(throw _, identity)

    if (preserveTimes && source.preserveCollectionTimestamps)
      assertNonFrozenCollectionMetadataReadable(
        connector,
        source.keyspace,
        source.table,
        nonFrozenCollectionColumns(tableDef)
      )

    val selectCassandraRDD = spark.sparkContext
      .cassandraTable[CassandraSQLRow](
        source.keyspace,
        source.table,
        (s, e) => !tokenRangesToSkip.contains((s, e))
      )
      .withConnector(connector)
      .withReadConf(readConf)
      .select(selection.columnRefs: _*)

    val finalCassandraRDD = source.where match {
      case Some(filter) => selectCassandraRDD.where(filter)
      case None         => selectCassandraRDD
    }

    val rdd = finalCassandraRDD
      .asInstanceOf[RDD[Row]]
      .map { row =>
        Row.fromSeq(row.toSeq.map(v => widenTimestampValue(convertValue(v))))
      }

    val rawDataframe = spark.createDataFrame(rdd, selection.schema)

    if (skipExplosion) {
      SourceDataFrame(rawDataframe, selection.timestampColumns, source.supportsSavepoints)
    } else {
      selection.timestampColumns match {
        case None =>
          SourceDataFrame(rawDataframe, selection.timestampColumns, source.supportsSavepoints)
        case Some(TimestampColumns(ttl, writeTime)) =>
          val (primaryKeyOrdinals, regularKeyOrdinals) = indexFields(
            rawDataframe.schema.fields.map(_.name).toList,
            origSchema.fields.map(_.name).toList,
            tableDef
          )

          // Non-frozen collection columns whose per-element TTL/WRITETIME we preserve via
          // collection-append passes. They are excluded from the base (scalar/frozen) explode.
          val perElementNames =
            if (source.preserveCollectionTimestamps)
              nonFrozenCollectionColumns(tableDef).map(_.columnName).toSet
            else Set.empty[String]

          val baseOrigSchema =
            StructType(origSchema.fields.filterNot(f => perElementNames.contains(f.name)))
          val baseRegularKeyOrdinals =
            regularKeyOrdinals.filterNot { case (name, _) => perElementNames.contains(name) }

          val broadcastPrimaryKeyOrdinals = spark.sparkContext.broadcast(primaryKeyOrdinals)
          val broadcastRegularKeyOrdinals = spark.sparkContext.broadcast(baseRegularKeyOrdinals)
          val broadcastSchema = spark.sparkContext.broadcast(baseOrigSchema)
          val finalSchema = StructType(
            baseOrigSchema.fields ++
              Seq(StructField(ttl, IntegerType, true), StructField(writeTime, LongType, true))
          )

          log.info("Schema that'll be used for writing to Scylla:")
          log.info(finalSchema.treeString)

          val explodedRdd = rawDataframe.rdd.flatMap { row =>
            explodeBaseRow(
              row,
              broadcastSchema.value,
              broadcastPrimaryKeyOrdinals.value,
              broadcastRegularKeyOrdinals.value
            )
          }

          val collectionAppendWrites =
            if (perElementNames.isEmpty) Nil
            else
              buildCollectionAppendWrites(
                spark,
                rawDataframe,
                tableDef,
                origSchema,
                primaryKeyOrdinals,
                regularKeyOrdinals,
                perElementNames
              )

          SourceDataFrame(
            rawDataframe,
            selection.timestampColumns,
            source.supportsSavepoints,
            Some((explodedRdd, finalSchema)),
            collectionAppendWrites
          )
      }
    }
  }
}
