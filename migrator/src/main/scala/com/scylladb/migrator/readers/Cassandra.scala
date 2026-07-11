package com.scylladb.migrator.readers

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.{ CassandraConnector, ColumnDef, Schema, TableDef }
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.oss.driver.api.core.cql.{ PreparedStatement, Row => DriverRow }
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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.cassandra.{ CassandraSQLRow, DataTypeConverter }
import org.apache.spark.sql.types.{
  ArrayType,
  ByteType,
  DataType,
  IntegerType,
  LongType,
  MapType,
  MetadataBuilder,
  ShortType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.unsafe.types.UTF8String
import com.scylladb.migrator.ConsistencyLevelUtils
import com.scylladb.migrator.scylla.{ CollectionAppendWrite, SourceDataFrame }

import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import java.nio.charset.StandardCharsets

object Cassandra {
  val log = LogManager.getLogger("com.scylladb.migrator.readers.Cassandra")

  /** Above this element count, a single non-frozen collection cell is logged as a potential
    * memory/throughput hot spot: per-element TTL/WRITETIME preservation may expand it into up to
    * this many separate collection-append updates (one per distinct TTL/WRITETIME group).
    */
  private val LargeCollectionWarnThreshold = 100000

  /** Hard fail-safe on the number of elements in a single non-frozen collection cell. Beyond this,
    * per-element expansion is treated as pathological (unbounded executor memory / write
    * amplification) and the migration aborts rather than risking an OOM mid-run. Override with
    * `-Dscylla.migrator.maxCollectionElements=<n>` (a non-positive value disables the cap).
    */
  private val LargeCollectionHardLimit: Int =
    sys.props
      .get("scylla.migrator.maxCollectionElements")
      .flatMap(s => scala.util.Try(s.trim.toInt).toOption)
      .getOrElse(1000000)

  /** Max number of collection keys read per subscript point-read statement (ScyllaDB 2026.2+ path).
    * Each key contributes two bind markers (`WRITETIME(col[?]), TTL(col[?])`), so a single
    * statement would otherwise mint `2*N` markers for an `N`-element cell and blow past the
    * native-protocol limit (65535) around 32k elements — long before [[LargeCollectionHardLimit]].
    * Reading in fixed-width chunks bounds both the marker count (`2*chunk`) and the number of
    * distinct prepared statements (one per chunk length). Override with
    * `-Dscylla.migrator.subscriptKeyChunkSize=<n>`.
    */
  private val SubscriptKeyChunkSize: Int =
    sys.props
      .get("scylla.migrator.subscriptKeyChunkSize")
      .flatMap(s => scala.util.Try(s.trim.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(100)

  case class Selection(
    columnRefs: List[ColumnRef],
    schema: StructType,
    timestampColumns: Option[TimestampColumns]
  )

  /** How the source server exposes per-element `WRITETIME()`/`TTL()` for non-frozen collections.
    *
    *   - [[ArrayMetadataRead]]: Cassandra 5.0+ — the collection-wide `WRITETIME(col)`/`TTL(col)`
    *     form returns element-aligned lists in a single scan (the default, cheapest path).
    *   - [[SubscriptMetadataRead]]: ScyllaDB 2026.2+ — only the per-element subscript form
    *     `WRITETIME(col[key])`/`TTL(col[key])` is accepted, so element metadata is fetched via a
    *     per-row point read (see [[readRawRddSubscript]]).
    */
  sealed trait MetadataReadStrategy
  case object ArrayMetadataRead extends MetadataReadStrategy
  case object SubscriptMetadataRead extends MetadataReadStrategy

  /** Regular columns that are non-frozen (multi-cell) collections. These keep per-element
    * TTL/WRITETIME (one metadata value per element), so `TTL()`/`WRITETIME()` on them return a list
    * aligned with the collection's elements rather than a scalar.
    */
  def nonFrozenCollectionColumns(tableDef: TableDef): Seq[ColumnDef] =
    tableDef.regularColumns.filter(c => c.columnType.isCollection && c.columnType.isMultiCell)

  private def quoteCqlIdentifier(id: String): String =
    "\"" + id.replace("\"", "\"\"") + "\""

  /** Decide how to read per-element collection metadata from the source by PREPARing (no execution,
    * no data read) probe statements, so the choice is made before the distributed read:
    *
    *   1. `WRITETIME(col), TTL(col)` (whole-collection list form) — Cassandra 5.0+ ⇒
    *      [[ArrayMetadataRead]].
    *   2. else `WRITETIME(col[?]), TTL(col[?])` (per-element subscript form) — ScyllaDB 2026.2+ ⇒
    *      [[SubscriptMetadataRead]].
    *   3. else neither is accepted ⇒ fail fast with the same actionable error as the array probe.
    *
    * `col` is the first non-frozen collection column; all such columns share the same server, so a
    * single probe determines the strategy for the table.
    */
  private[migrator] def detectMetadataReadStrategy(
    connector: CassandraConnector,
    keyspace: String,
    table: String,
    nonFrozen: Seq[ColumnDef]
  ): MetadataReadStrategy =
    nonFrozen.headOption match {
      case None => ArrayMetadataRead
      case Some(col) =>
        val c = quoteCqlIdentifier(col.columnName)
        val from =
          s"FROM ${quoteCqlIdentifier(keyspace)}.${quoteCqlIdentifier(table)} LIMIT 1"
        def canPrepare(cql: String): Boolean =
          Try(connector.withSessionDo(_.prepare(cql))).isSuccess

        if (canPrepare(s"SELECT WRITETIME($c), TTL($c) $from"))
          ArrayMetadataRead
        else if (canPrepare(s"SELECT WRITETIME($c[?]), TTL($c[?]) $from"))
          SubscriptMetadataRead
        else
          throw new IllegalStateException(
            s"preserveCollectionTimestamps is enabled, but the source cannot read per-element " +
              s"WRITETIME()/TTL() on non-frozen collection column '${col.columnName}' via either " +
              s"the collection-wide form (Cassandra 5.0+) or the element-subscript form " +
              s"(ScyllaDB 2026.2+). This feature requires one of those source versions."
          )
    }

  /** Bind a runtime value into a driver statement by its Java runtime class. Collection keys are
    * limited to the text/int family (see [[mapKeyOrdering]]), which already map to default driver
    * codecs. Primary-key values are pre-converted by the caller for the two widened/decoded types
    * that would otherwise miss a codec (`timestamp`->Instant, `blob`->ByteBuffer); all other PK
    * types reach here already codec-compatible.
    */
  private def driverBindValue(v: Any): AnyRef = v match {
    case null                 => null
    case s: String            => s
    case n: java.lang.Integer => n
    case n: java.lang.Long    => n
    case n: java.lang.Short   => n
    case n: java.lang.Byte    => n
    case i: Int               => java.lang.Integer.valueOf(i)
    case l: Long              => java.lang.Long.valueOf(l)
    case other                => other.asInstanceOf[AnyRef]
  }

  /** Read the source into an `RDD[Row]` matching `selection.schema` when the server only supports
    * the per-element SUBSCRIPT form of `WRITETIME()`/`TTL()` (ScyllaDB 2026.2+).
    *
    * Two phases, producing exactly the same array-sidecar schema as the Cassandra 5.0 array path so
    * every downstream stage (explode, multi-pass write, validation) is reused unchanged:
    *   1. Base scan (connector): PK + all regular column VALUES + scalar `WRITETIME`/`TTL` sidecars
    *      (Scylla supports scalar). Non-frozen collection metadata columns are omitted here.
    *   2. Per-row point read (driver): for each non-frozen collection, sort its decoded
    *      elements/entries with the column's ordering (so the arrays align with the order the
    *      downstream explode re-sorts into), then one `SELECT WRITETIME(col[?]), TTL(col[?]), ...`
    *      point read fetches all element metadata, assembled into element-aligned arrays.
    */
  private def readRawRddSubscript(
    spark: SparkSession,
    connector: CassandraConnector,
    source: SourceSettings.Cassandra,
    readConf: ReadConf,
    tableDef: TableDef,
    selection: Selection,
    nonFrozen: Seq[ColumnDef],
    tokenRangesToSkip: Set[(Token[_], Token[_])]
  ): RDD[Row] = {
    val nfNames = nonFrozen.map(_.columnName).toSet

    // Phase 1 selection: keep every column value + scalar sidecars, but DROP the per-element
    // collection sidecars (Scylla rejects `WRITETIME(col)`/`TTL(col)` on non-frozen collections).
    val baseColumnRefs: List[ColumnRef] =
      (tableDef.partitionKey.map(_.ref) ++
        tableDef.clusteringColumns.map(_.ref) ++
        tableDef.regularColumns.flatMap { column =>
          val colName = column.columnName
          if (nfNames.contains(colName)) List(column.ref)
          else
            List(
              column.ref,
              colName.ttl as s"${colName}_ttl",
              colName.writeTime as s"${colName}_writetime"
            )
        }).toList

    val baseSchema = StructType(selection.schema.fields.filterNot { f =>
      nfNames.exists(n => f.name == s"${n}_ttl" || f.name == s"${n}_writetime")
    })

    val baseSelectRDD = spark.sparkContext
      .cassandraTable[CassandraSQLRow](
        source.keyspace,
        source.table,
        (s, e) => !tokenRangesToSkip.contains((s, e))
      )
      .withConnector(connector)
      .withReadConf(readConf)
      .select(baseColumnRefs: _*)

    val baseFilteredRDD = source.where match {
      case Some(filter) => baseSelectRDD.where(filter)
      case None         => baseSelectRDD
    }

    val baseRdd = baseFilteredRDD
      .asInstanceOf[RDD[Row]]
      .map(row => Row.fromSeq(row.toSeq.map(v => widenTimestampValue(convertValue(v)))))

    // Everything below is captured by the executor closure, so keep it serializable (primitives,
    // Strings, the Serializable `ElementSorter`, and the Serializable `connector`).
    val baseFieldIndex: Map[String, Int] = baseSchema.fieldNames.zipWithIndex.toMap
    val pkColumns: Seq[ColumnDef] = tableDef.partitionKey ++ tableDef.clusteringColumns
    val pkNames: Seq[String] = pkColumns.map(_.columnName)
    val pkOrdinals: Seq[Int] = pkNames.map(baseFieldIndex)
    // Per-PK bind conversion tag. The base scan already widened `timestamp`->Long(epoch-ms) and
    // yields `blob`->Array[Byte], neither of which has a matching driver default codec, so binding
    // them by runtime class fails (Long is bound as bigint, byte[] has no codec). Convert those two
    // back to the driver-native type (Instant / ByteBuffer) before binding; everything else
    // (text/int/bigint/uuid/inet/decimal/varint/boolean/float/double) is already codec-compatible.
    //   0 = pass-through, 1 = timestamp(Long->Instant), 2 = blob(Array[Byte]->ByteBuffer)
    val pkBindKinds: Seq[Int] = pkColumns.map { c =>
      c.columnType match {
        case com.datastax.spark.connector.types.TimestampType => 1
        case com.datastax.spark.connector.types.BlobType      => 2
        case _                                                => 0
      }
    }
    // (columnName, ordinal-in-base-row, isMap, elementSorter)
    val nfInfo: Seq[(String, Int, Boolean, ElementSorter[_])] =
      nonFrozen.map { c =>
        val sorter = collectionElementOrdering(c.columnType).getOrElse(
          throw new IllegalStateException(
            s"Non-frozen collection '${c.columnName}' has no supported element ordering; " +
              "this should have been rejected by determineCopyType."
          )
        )
        (
          c.columnName,
          baseFieldIndex(c.columnName),
          c.columnType.isInstanceOf[CqlMapType[_, _]],
          sorter
        )
      }
    val keyspaceQ = quoteCqlIdentifier(source.keyspace)
    val tableQ = quoteCqlIdentifier(source.table)
    val whereClause = pkNames.map(n => s"${quoteCqlIdentifier(n)} = ?").mkString(" AND ")
    // Point reads must honor the same source consistency level as the base scan; otherwise the
    // driver's session default (LOCAL_ONE) is used, widening the read-consistency race window
    // between the base scan and the metadata point read.
    val pointReadConsistencyLevel = readConf.consistencyLevel
    val targetFields = selection.schema.fields
    // Map a target array-sidecar field name back to its collection column, if any.
    def collectionOfSidecar(fieldName: String): Option[(String, Boolean)] =
      nfNames.collectFirst {
        case n if fieldName == s"${n}_ttl"       => (n, true)
        case n if fieldName == s"${n}_writetime" => (n, false)
      }

    baseRdd.mapPartitions { rows =>
      // Hold ONE session open for the whole partition and stream rows lazily (no `.toList`, which
      // would materialize every output row of the partition in executor memory). The session is
      // borrowed from the connector's pool and returned when the Spark task completes.
      val session = connector.openSession()
      val stmtCache = mutable.Map.empty[(String, Int), PreparedStatement]
      val dropped = new java.util.concurrent.atomic.AtomicLong(0L)
      val taskCtxOpt = Option(org.apache.spark.TaskContext.get())
      taskCtxOpt.foreach(_.addTaskCompletionListener[Unit] { _ =>
        if (dropped.get() > 0L)
          log.warn(
            s"Subscript metadata read dropped ${dropped.get()} collection element(s) whose " +
              s"per-element WRITETIME was null at point-read time (element absent — e.g. a " +
              s"concurrent delete). The migrator requires a quiescent source; resume/re-run once " +
              s"writes have stopped if this is unexpected."
          )
        session.close()
      })

      val pkBindKindsArr = pkBindKinds.toArray
      val pkOrdinalsArr = pkOrdinals.toArray

      // Per-column: the (possibly filtered) collection VALUE plus element-aligned TTL/WRITETIME
      // arrays. Value and metadata are re-derived from the SAME sorted key set, so they always
      // align; any element whose WRITETIME reads back null is dropped from both.
      def readColumn(
        row: Row,
        name: String,
        ord: Int,
        isMap: Boolean,
        sorter: ElementSorter[_],
        pkBinds: Seq[AnyRef]
      ): (Any, Seq[Integer], Seq[java.lang.Long]) = {
        if (row.isNullAt(ord)) return (null, null, null)

        // (subscript-key, retained-value): for a map the value is the map value; for a set/list the
        // element is its own key and value.
        val entries: IndexedSeq[(Any, Any)] =
          if (isMap)
            sorter.sortEntries(
              row.get(ord).asInstanceOf[scala.collection.Map[Any, Any]].toIndexedSeq
            )
          else
            sorter
              .sortElements(row.get(ord).asInstanceOf[scala.collection.Seq[Any]].toIndexedSeq)
              .map(e => (e, e))

        val n = entries.length
        if (n == 0)
          return (row.get(ord), IndexedSeq.empty[Integer], IndexedSeq.empty[java.lang.Long])

        val colQ = quoteCqlIdentifier(name)
        val ttls = new Array[Integer](n)
        val wts = new Array[java.lang.Long](n)

        // Read metadata in fixed-width chunks so a huge cell never exceeds the native-protocol bind
        // marker limit and only mints a bounded set of prepared statements (one per chunk length).
        var base = 0
        while (base < n) {
          val chunkLen = math.min(SubscriptKeyChunkSize, n - base)
          val ps = stmtCache.getOrElseUpdate(
            (name, chunkLen), {
              val projection =
                (0 until chunkLen).map(_ => s"WRITETIME($colQ[?]), TTL($colQ[?])").mkString(", ")
              session.prepare(s"SELECT $projection FROM $keyspaceQ.$tableQ WHERE $whereClause")
            }
          )
          // Bind order follows statement text: each key twice (WRITETIME, TTL), then the PK values.
          val keyBinds =
            (0 until chunkLen).flatMap { j =>
              val k = entries(base + j)._1
              Seq(driverBindValue(k), driverBindValue(k))
            }
          val bound = ps
            .bind((keyBinds ++ pkBinds).toArray: _*)
            .setConsistencyLevel(pointReadConsistencyLevel)

          val dr: DriverRow = session.execute(bound).one()
          var j = 0
          while (j < chunkLen) {
            val wtIdx = 2 * j
            val ttlIdx = 2 * j + 1
            wts(base + j) =
              if (dr == null || dr.isNull(wtIdx)) null
              else java.lang.Long.valueOf(dr.getLong(wtIdx))
            ttls(base + j) =
              if (dr == null || dr.isNull(ttlIdx)) null
              else Integer.valueOf(dr.getInt(ttlIdx))
            j += 1
          }
          base += chunkLen
        }

        // A null WRITETIME means the element no longer exists at the point-read snapshot (element
        // absent, e.g. a concurrent delete). Drop it from BOTH the value and the metadata so the
        // arrays stay aligned and the job does not abort. Under a quiescent source (the documented
        // requirement) `keep.length == n`, so the value/metadata are byte-for-byte the base scan.
        // NOTE: a null TTL is retained — it legitimately means "no TTL / permanent element".
        val keptIdx = (0 until n).filter(i => wts(i) != null)
        if (keptIdx.length == n) {
          (row.get(ord), ArraySeq.unsafeWrapArray(ttls), ArraySeq.unsafeWrapArray(wts))
        } else {
          dropped.addAndGet((n - keptIdx.length).toLong)
          val value: Any =
            if (isMap)
              keptIdx.map(i => entries(i)._1 -> entries(i)._2).to(scala.collection.immutable.Map)
            else
              keptIdx.map(i => entries(i)._2).toVector
          (
            value,
            ArraySeq.unsafeWrapArray(keptIdx.map(ttls).toArray),
            ArraySeq.unsafeWrapArray(keptIdx.map(wts).toArray)
          )
        }
      }

      rows.map { row =>
        // PK binds are identical for every collection column in a row, so compute them once.
        val pkBinds: Seq[AnyRef] =
          pkOrdinalsArr.indices.map { i =>
            val o = pkOrdinalsArr(i)
            val raw = if (row.isNullAt(o)) null else row.get(o)
            val converted: Any = (pkBindKindsArr(i), raw) match {
              case (_, null)              => null
              case (1, l: Long)           => java.time.Instant.ofEpochMilli(l)
              case (1, l: java.lang.Long) => java.time.Instant.ofEpochMilli(l.longValue)
              case (2, b: Array[Byte])    => java.nio.ByteBuffer.wrap(b)
              case (_, v)                 => v
            }
            driverBindValue(converted)
          }

        val meta: Map[String, (Any, Seq[Integer], Seq[java.lang.Long])] =
          nfInfo.map { case (name, ord, isMap, sorter) =>
            name -> readColumn(row, name, ord, isMap, sorter, pkBinds)
          }.toMap

        val values = targetFields.map { f =>
          collectionOfSidecar(f.name) match {
            case Some((col, isTtl)) =>
              val (_, ttlArr, wtArr) = meta(col)
              if (isTtl) ttlArr else wtArr
            case None =>
              if (nfNames.contains(f.name)) meta(f.name)._1
              else row.get(baseFieldIndex(f.name))
          }
        }
        Row.fromSeq(ArraySeq.unsafeWrapArray(values.asInstanceOf[Array[Any]]))
      }
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

  /** Sorts a non-frozen collection's elements/entries into Cassandra's on-disk order using
    * decorate-sort-undecorate: each value's comparison key (e.g. the UTF-8 bytes of a text key) is
    * computed ONCE per element up front rather than re-derived on every pairwise comparison.
    *
    * A plain `Ordering.by(f)` runs `f` inside every `compare`, so an O(N log N) sort would call `f`
    * ~2·N·log N times. For text keys `f` allocates a `String` + `byte[]` each call, so a 1000-key
    * `map<text,_>` cell would churn ~20k transient arrays just to sort one cell. Decorating first
    * makes that exactly N encodings.
    */
  private[migrator] final class ElementSorter[K](keyOf: Any => K)(implicit ord: Ordering[K])
      extends Serializable {
    private def sortByPrecomputedKey[V](xs: IndexedSeq[V], project: V => Any): IndexedSeq[V] =
      xs.map(v => keyOf(project(v)) -> v).sortBy(_._1)(ord).map(_._2)

    def sortEntries(entries: IndexedSeq[(Any, Any)]): IndexedSeq[(Any, Any)] =
      sortByPrecomputedKey[(Any, Any)](entries, _._1)

    def sortElements(elements: IndexedSeq[Any]): IndexedSeq[Any] =
      sortByPrecomputedKey[Any](elements, identity)
  }

  private def textElementSorter: ElementSorter[Array[Byte]] =
    new ElementSorter[Array[Byte]](_.toString.getBytes(StandardCharsets.UTF_8))(
      unsignedBytesOrdering
    )

  private def longElementSorter: ElementSorter[Long] =
    new ElementSorter[Long](_.asInstanceOf[Number].longValue())(Ordering.Long)

  /** Sorter over runtime map-key values that reproduces Cassandra's on-disk key order for the
    * supported key types, so a decoded map (whose iteration order Spark/Catalyst does not preserve)
    * can be re-aligned with the element-order `WRITETIME`/`TTL` lists. Returns `None` for key types
    * whose Cassandra comparator we cannot faithfully reproduce here.
    */
  private[migrator] def mapKeyOrdering(keyType: ColumnType[_]): Option[ElementSorter[_]] =
    keyType match {
      case TextType | AsciiType | VarCharType =>
        Some(textElementSorter)
      case IntType | BigIntType | SmallIntType | TinyIntType =>
        Some(longElementSorter)
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
  private def collectionElementOrdering(columnType: ColumnType[_]): Option[ElementSorter[_]] =
    columnType match {
      case m: CqlMapType[_, _] => mapKeyOrdering(m.keyType)
      case s: CqlSetType[_]    => mapKeyOrdering(s.elemType)
      case _                   => None
    }

  /** Spark-schema counterpart of [[mapKeyOrdering]]. When re-hydrating per-element collection
    * metadata from Parquet we no longer have the Cassandra [[ColumnType]], only the Spark
    * [[DataType]] of the collection's key (maps) or element (sets). Must stay byte-for-byte
    * consistent with [[mapKeyOrdering]] so the Parquet round-trip re-aligns elements with their
    * metadata exactly as the direct Cassandra->Scylla path does. Returns `None` for unsupported
    * types (which are rejected before ever being written with array sidecars).
    */
  private def sparkKeyOrdering(dataType: DataType): Option[ElementSorter[_]] = dataType match {
    case StringType =>
      Some(textElementSorter)
    case IntegerType | LongType | ShortType | ByteType =>
      Some(longElementSorter)
    case _ => None
  }

  /** Whether a Spark field carries per-element collection metadata, i.e. its companion `<name>_ttl`
    * sidecar is an [[ArrayType]] (one TTL per collection element) rather than a scalar (frozen
    * collection / scalar column). Used on the Parquet restore path to tell the two apart.
    */
  private def isPerElementCollectionMetaType(ttlFieldType: DataType): Boolean =
    ttlFieldType.isInstanceOf[ArrayType]

  /** Spark field-metadata key stamped on a non-frozen collection column when it is exported to
    * Parquet with per-element TTL/WRITETIME array sidecars. Its value is the CQL collection kind
    * ([[CollectionKindSet]]/[[CollectionKindMap]]). Spark persists field metadata in the Parquet
    * footer schema, so it round-trips back on restore.
    *
    * This is the trust anchor for the Parquet restore path: an [[ArrayType]] alone cannot tell a
    * CQL `set` (supported) from a `list` (unsupported — order/duplicate semantics differ) or from
    * unrelated foreign array data. Requiring this marker means only Parquet actually produced by a
    * `preserveCollectionTimestamps` export is ever replayed as a per-element collection-append.
    */
  private[migrator] val CollectionKindMetaKey = "scylla.migrator.collectionKind"
  private[migrator] val CollectionKindSet = "set"
  private[migrator] val CollectionKindMap = "map"

  /** The CQL collection kind we stamp for a non-frozen collection column, or `None` for kinds we do
    * not export with per-element metadata (e.g. lists are rejected earlier).
    */
  private def exportCollectionKind(columnType: ColumnType[_]): Option[String] =
    columnType match {
      case _: CqlMapType[_, _] => Some(CollectionKindMap)
      case _: CqlSetType[_]    => Some(CollectionKindSet)
      case _                   => None
    }

  /** Tag a non-frozen collection column's Spark field with its CQL kind so the Parquet restore path
    * (and the validator repair path, which builds an equivalent schema) can validate it (see
    * [[CollectionKindMetaKey]]). No-op for column types without a per-element collection kind.
    */
  private[migrator] def taggedCollectionField(
    field: StructField,
    columnType: ColumnType[_]
  ): StructField =
    exportCollectionKind(columnType).fold(field) { kind =>
      val md = new MetadataBuilder()
        .withMetadata(field.metadata)
        .putString(CollectionKindMetaKey, kind)
        .build()
      field.copy(metadata = md)
    }

  private def tagCollectionKind(field: StructField, tableDef: TableDef): StructField =
    tableDef.regularColumns
      .find(_.columnName == field.name)
      .fold(field)(c => taggedCollectionField(field, c.columnType))

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

    // Timestamp preservation appends internal metadata columns named `ttl`/`writetime`, plus a
    // `<col>_ttl`/`<col>_writetime` sidecar for every regular column. `ttl` and `writetime` are
    // non-reserved CQL keywords, so a real column may legitimately be named `ttl`/`writetime` (or
    // `<col>_ttl`-shaped). Such a collision silently misclassifies a real column as metadata
    // (dropping it from the INSERT or misrouting the collection-append write), so reject it up
    // front with a clear, actionable error instead of corrupting data.
    val reservedInternalNames =
      Set("ttl", "writetime") ++
        tableDef.regularColumns.flatMap(c =>
          Seq(s"${c.columnName}_ttl", s"${c.columnName}_writetime")
        )
    val reservedNameCollisions =
      tableDef.columns.map(_.columnName).filter(reservedInternalNames.contains).distinct

    // F12: `preserveCollectionTimestamps` only takes effect alongside `preserveTimestamps`. If the
    // operator enabled it without the master switch, per-element collection TTL/WRITETIME would be
    // silently NOT preserved; warn so the misconfiguration is visible.
    if (preserveCollectionTimesRequest && !preserveTimesRequest)
      log.warn(
        "'preserveCollectionTimestamps' is enabled but 'preserveTimestamps' is disabled, so it has " +
          "no effect: per-element collection TTL/WRITETIME will NOT be preserved. Set " +
          "'preserveTimestamps' to true to enable it."
      )

    if (preserveTimesRequest && reservedNameCollisions.nonEmpty)
      Left(
        new Exception(
          "TTL/Writetime preservation reserves the internal column names 'ttl', 'writetime', and " +
            "'<column>_ttl'/'<column>_writetime'. The source table has column(s) colliding with " +
            s"these reserved names: ${reservedNameCollisions.mkString(", ")}. Rename the source " +
            "column(s), or set 'preserveTimestamps' to false to continue."
        )
      )
    else if (nonFrozen.nonEmpty && preserveTimesRequest && !preserveCollectionTimesRequest)
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
          // Stamp the CQL collection kind on per-element collection columns so the Parquet restore
          // path can distinguish a genuine migrator export from foreign array data (see
          // CollectionKindMetaKey). Inert on the direct Cassandra->Scylla path.
          baseField = if (isPerElementCollection) tagCollectionKind(origField, tableDef)
                      else origField
          field <- if (isRegular)
                     List(
                       baseField,
                       StructField(s"${origField.name}_ttl", ttlType, true),
                       StructField(s"${origField.name}_writetime", writetimeType, true)
                     )
                   else List(baseField)
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
  /** Largest per-element WRITETIME across the given per-element collection sidecar ordinals of a
    * wide row, or `None` when no such collection is populated. Used to keep base marker rows
    * consistent with the collection-append passes (F11 / A2).
    */
  private def maxPerElementWritetime(row: Row, ordinals: Seq[Int]): Option[Long] = {
    val wts = ordinals.iterator.flatMap { o =>
      if (o < 0 || o >= row.length || row.isNullAt(o)) Iterator.empty
      else asNumberSeq(row.get(o)).iterator.collect { case n: Number => n.longValue() }
    }
    if (wts.hasNext) Some(wts.max) else None
  }

  def explodeBaseRow(
    row: Row,
    baseSchema: StructType,
    primaryKeyOrdinals: Map[String, Int],
    baseRegularKeyOrdinals: Map[String, (Int, Int, Int)],
    perElementWritetimeOrdinals: Seq[Int] = Nil,
    perElementTtlOrdinals: Seq[Int] = Nil
  ): Iterable[Row] =
    if (baseRegularKeyOrdinals.nonEmpty) {
      val rows = explodeRow(row, baseSchema, primaryKeyOrdinals, baseRegularKeyOrdinals)
      // A2: mixed scalar+collection table. A row whose scalar columns are all null yields a base
      // marker whose WRITETIME group is `Unset` (server "now"), while its collection appends use the
      // older source per-element WRITETIMEs — the same phantom-live-but-empty-row risk F11 fixes for
      // collection-only tables. Floor any `Unset` marker writetime at the max per-element WRITETIME.
      // The trailing element of each exploded row is its writetime; `Unset` there means the group's
      // columns were all null (a live scalar cell always has a writetime).
      maxPerElementWritetime(row, perElementWritetimeOrdinals) match {
        case Some(wt) =>
          rows.map { r =>
            val vals = r.toSeq.toArray
            val wtIdx = vals.length - 1
            if (vals(wtIdx) == CassandraOption.Unset) {
              vals(wtIdx) = java.lang.Long.valueOf(wt)
              Row(ArraySeq.unsafeWrapArray(vals): _*)
            } else r
          }
        case None => rows
      }
    } else {
      // F11: collection-only table (the only regular columns are per-element collections). The base
      // write emits a single primary-key-only row so the partition/row exists. Using an Unset
      // writetime would stamp this marker with the server "now", which can beat a source-side
      // tombstone that the source per-element WRITETIMEs (used by the appends) cannot beat, leaving
      // a phantom live-but-empty row. Instead stamp the marker with the max per-element WRITETIME so
      // it is consistent with the collection-append passes.
      val markerWritetime: AnyRef =
        maxPerElementWritetime(row, perElementWritetimeOrdinals)
          .map(java.lang.Long.valueOf)
          .getOrElse(CassandraOption.Unset)
      // C1: mirror the marker's TTL to the collection's latest-cell expiry. A hardcoded TTL 0 (=no
      // TTL) makes the primary-key marker permanently live, so after every TTL'd element expires the
      // target keeps an empty, never-expiring row that the source (whose liveness came only from the
      // now-expired cells) no longer has. Row-marker liveness must reflect the LATEST cell expiry:
      // permanent if any element is permanent (TTL 0/absent), else the max finite element TTL.
      val markerTtl: Integer = {
        val ttls = perElementTtlOrdinals.iterator.flatMap { o =>
          if (o < 0 || o >= row.length || row.isNullAt(o)) Iterator.empty
          else
            asNumberSeq(row.get(o)).iterator.map {
              case n: Number => n.intValue()
              case _         => 0 // null TTL slot => no TTL => permanent
            }
        }.toVector
        if (ttls.isEmpty || ttls.exists(_ <= 0)) Integer.valueOf(0)
        else Integer.valueOf(ttls.max)
      }
      val newValues = baseSchema.fields.map { field =>
        primaryKeyOrdinals
          .get(field.name)
          .map(ord => if (row.isNullAt(ord)) null else convertValue(row.get(ord)))
          .getOrElse(CassandraOption.Unset)
      } ++ Seq(markerTtl, markerWritetime)
      List(Row(ArraySeq.unsafeWrapArray(newValues): _*))
    }

  private def asNumberSeq(v: Any): IndexedSeq[Any] = v match {
    case s: scala.collection.Seq[Any] => s.toIndexedSeq
    case a: Array[_]                  => ArraySeq.unsafeWrapArray(a.asInstanceOf[Array[Any]])
    case _                            => IndexedSeq.empty
  }

  /** Build the per-element collection-append rows for one non-frozen collection column of a single
    * wide source row. Each emitted row is `[primary key values..., grouped collection value, ttl,
    * writetime]`. The decoded collection is re-sorted with `elementSorter` (set elements by value,
    * map entries by key) to reproduce Cassandra's on-disk element order, so element[i] pairs with
    * its own element-order `TTL()`/`WRITETIME()[i]`. Elements are then grouped by
    * `(ttl, writetime)` so co-timestamped elements share one append.
    */
  def collectionAppendRows(
    row: Row,
    pkOrdinals: Array[Int],
    colOrdinal: Int,
    ttlOrdinal: Int,
    writetimeOrdinal: Int,
    isMap: Boolean,
    elementSorter: Option[ElementSorter[_]]
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
      if (i < ttlSeq.length && ttlSeq(i) != null) {
        // Read as Long first: on the Parquet restore path the TTL sidecar may be a LongType array,
        // and `intValue()` would silently wrap an out-of-range value. CQL TTL is a non-negative
        // 32-bit second count, so anything outside [0, Int.MaxValue] is malformed (e.g. foreign /
        // hand-crafted Parquet); fail loudly rather than truncate.
        val n = ttlSeq(i).asInstanceOf[Number].longValue()
        if (n < 0 || n > Int.MaxValue)
          throw new IllegalStateException(
            s"Per-element TTL at index $i is $n, outside the valid CQL TTL range " +
              s"[0, ${Int.MaxValue}] seconds. Refusing to silently truncate a malformed TTL."
          )
        n.toInt
      } else 0
    // A present collection element always has a WRITETIME on Cassandra 5.0+/modern ScyllaDB. A
    // null/non-numeric slot would leave the element with no timestamp to preserve; dropping it
    // silently (the previous `Option`-in-`flatMap` behaviour) is data loss, so fail loudly instead.
    def wtAt(i: Int): Long =
      wtSeq(i) match {
        case n: Number => n.longValue()
        case other =>
          throw new IllegalStateException(
            s"Per-element WRITETIME at index $i was ${if (other == null) "null" else other} for a " +
              "non-frozen collection element; the element cannot be written with its original " +
              "timestamp. Aborting to avoid silently dropping it."
          )
      }

    // Enforce the size cap on the RAW decoded collection size, before any copy/sort/group, so a
    // pathological cell fails fast instead of OOMing during `toIndexedSeq`/`sortElements`.
    def enforceSizeLimits(size: Int): Unit = {
      if (LargeCollectionHardLimit > 0 && size > LargeCollectionHardLimit)
        throw new IllegalStateException(
          s"Non-frozen collection cell has $size elements, exceeding the hard limit of " +
            s"$LargeCollectionHardLimit. Per-element TTL/WRITETIME preservation expands each element " +
            "into collection-append updates, and a collection this large risks executor OOM / write " +
            "amplification. Raise or disable the cap with -Dscylla.migrator.maxCollectionElements=<n> " +
            "(non-positive disables) if this size is expected."
        )
      if (size > LargeCollectionWarnThreshold)
        log.warn(
          s"Non-frozen collection cell has $size elements; per-element TTL/WRITETIME " +
            "preservation may expand it into up to that many separate collection-append updates " +
            "(one per distinct TTL/WRITETIME group). Very large collections can cause high " +
            "executor memory/GC pressure and slow writes."
        )
    }

    def rowsFrom(elements: IndexedSeq[Any], rebuild: Seq[Any] => Any): Seq[Row] = {
      // Both sidecars must be element-aligned. On every real read path `TTL(col)` returns a list the
      // same length as the collection (null entries for elements with no TTL), so an empty/short TTL
      // sidecar alongside real writetimes signals malformed input (e.g. foreign/hand-crafted
      // Parquet). Do NOT treat a missing TTL array as "all permanent (TTL 0)" — that would silently
      // strip expiry from every element; fail loudly instead.
      require(
        wtSeq.length == elements.length && ttlSeq.length == elements.length,
        s"Collection metadata length mismatch for a per-element collection column: " +
          s"${elements.length} elements but ${wtSeq.length} writetimes / ${ttlSeq.length} ttls. " +
          "Element<->timestamp alignment cannot be guaranteed; aborting to avoid silent data loss."
      )
      elements.indices
        .groupBy(i => (ttlAt(i), wtAt(i)))
        .toSeq
        .map { case ((ttl, wt), indices) =>
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

    // Both the connector's decoded set and map lose their server (sorted) order, while the
    // WRITETIME()/TTL() lists arrive in that sorted order. Re-sort the elements/entries with the
    // Cassandra-compatible ordering so element[i] pairs with its own metadata[i].
    val sorter = elementSorter.getOrElse(
      throw new IllegalStateException(
        "Missing element ordering for a per-element collection column; this should have been " +
          "rejected earlier"
      )
    )

    if (isMap) {
      val m = row.get(colOrdinal).asInstanceOf[scala.collection.Map[Any, Any]]
      if (m.isEmpty) Nil
      else {
        enforceSizeLimits(m.size)
        val entries = sorter.sortEntries(m.toIndexedSeq)
        rowsFrom(
          entries.asInstanceOf[IndexedSeq[Any]],
          parts => parts.map(_.asInstanceOf[(Any, Any)]).toMap
        )
      }
    } else {
      val raw = row.get(colOrdinal).asInstanceOf[scala.collection.Seq[Any]]
      if (raw.isEmpty) Nil
      else {
        enforceSizeLimits(raw.size)
        rowsFrom(sorter.sortElements(raw.toIndexedSeq), parts => parts.toIndexedSeq)
      }
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
        val elementSorter = collectionElementOrdering(column.columnType)
        val pkOrdinalsBroadcast = spark.sparkContext.broadcast(pkOrdinalArray)
        val rdd = rawDataframe.rdd.flatMap { row =>
          collectionAppendRows(
            row,
            pkOrdinalsBroadcast.value,
            colOrd,
            ttlOrd,
            wtOrd,
            isMap,
            elementSorter
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

  /** Build one [[CollectionAppendWrite]] per per-element collection column found on the Parquet
    * restore path, using the Spark schema (rather than a Cassandra `TableDef`) to derive element
    * ordering and map/set shape. Mirrors [[buildCollectionAppendWrites]] but keyed off
    * [[sparkKeyOrdering]] so it stays consistent with the direct Cassandra->Scylla read path.
    */
  private def buildCollectionAppendWritesFromSchema(
    spark: SparkSession,
    df: DataFrame,
    origSchema: StructType,
    primaryKeyOrdinals: Map[String, Int],
    regularKeyOrdinals: Map[String, (Int, Int, Int)],
    perElementNames: Set[String]
  ): Seq[CollectionAppendWrite] = {
    // Deterministic primary-key ordering (by their ordinal in the wide row). The PK order among
    // themselves is irrelevant to the write (the column selector maps by name), it only needs to
    // agree between `pkOrdinalArray` and `pkFields`.
    val pkNames = primaryKeyOrdinals.toSeq.sortBy(_._2).map(_._1)
    val pkOrdinalArray = pkNames.map(primaryKeyOrdinals).toArray
    val pkFields = pkNames.map(name => origSchema(origSchema.fieldIndex(name)))

    perElementNames.toSeq.sorted.map { colName =>
      val (colOrd, ttlOrd, wtOrd) = regularKeyOrdinals(colName)
      val collectionField = origSchema(origSchema.fieldIndex(colName))
      val appendSchema = StructType(
        pkFields ++ Seq(
          collectionField,
          StructField("ttl", IntegerType, true),
          StructField("writetime", LongType, true)
        )
      )
      val (isMap, elementSorter): (Boolean, Option[ElementSorter[_]]) =
        collectionField.dataType match {
          case m: MapType   => (true, sparkKeyOrdering(m.keyType))
          case a: ArrayType => (false, sparkKeyOrdering(a.elementType))
          case _            => (false, None)
        }
      val pkOrdinalsBroadcast = spark.sparkContext.broadcast(pkOrdinalArray)
      val rdd = df.rdd.flatMap { row =>
        collectionAppendRows(
          row,
          pkOrdinalsBroadcast.value,
          colOrd,
          ttlOrd,
          wtOrd,
          isMap,
          elementSorter
        )
      }
      CollectionAppendWrite(colName, rdd, appendSchema)
    }
  }

  /** Collection-aware variant of [[explodeRowsFromPerColumnMeta]] for the Parquet restore path.
    *
    * Columns whose `<name>_ttl` sidecar is an [[ArrayType]] carry per-element collection metadata
    * (written by a `preserveCollectionTimestamps` Parquet export). Those columns are excluded from
    * the scalar base explode and instead replayed as per-element collection-append passes (matching
    * the direct Cassandra->Scylla multi-pass write), so the base RDD only ever contains scalar and
    * frozen-collection columns.
    *
    * When there are no such columns the base RDD/schema are identical to
    * [[explodeRowsFromPerColumnMeta]] and `collectionAppendWrites` is empty.
    *
    * @return
    *   base exploded rows, base write [[StructType]], [[TimestampColumns]], and the per-element
    *   collection-append passes.
    */
  def explodeRowsFromPerColumnMetaCollectionAware(
    spark: SparkSession,
    df: DataFrame
  ): (RDD[Row], StructType, TimestampColumns, Seq[CollectionAppendWrite]) = {
    val (primaryKeyOrdinals, regularKeyOrdinals) = indexFieldsFromSchema(df.schema)

    val metaColumns =
      regularKeyOrdinals.keys.flatMap(name => Set(s"${name}_ttl", s"${name}_writetime")).toSet
    val origSchema = StructType(df.schema.fields.filterNot(f => metaColumns.contains(f.name)))

    val timestampColumns = TimestampColumns("ttl", "writetime")

    // Regular columns whose TTL sidecar is an array => per-element collections. They are handled by
    // separate collection-append passes and excluded from the base explode.
    val perElementNames =
      regularKeyOrdinals.collect {
        case (name, (_, ttlOrd, _)) if isPerElementCollectionMetaType(df.schema(ttlOrd).dataType) =>
          name
      }.toSet

    // H4: this path may run against foreign / hand-crafted Parquet. Before we trust the array-typed
    // `<col>_ttl` sidecar as a per-element collection, validate that the sidecar pair and the base
    // column are internally consistent, so a malformed export fails loudly here rather than
    // producing silently corrupt appends (or an obscure crash) deep inside the write.
    perElementNames.toSeq.sorted.foreach { name =>
      val (colOrd, ttlOrd, wtOrd) = regularKeyOrdinals(name)
      val baseField = df.schema(colOrd)
      // H2/foreign-Parquet trust anchor: an ArrayType sidecar alone cannot tell a CQL set (safe to
      // replay via `col = col + ?`) from a list (ordered, duplicate-preserving, NOT idempotent) or
      // from unrelated foreign array data. Only Parquet produced by a preserveCollectionTimestamps
      // export carries the collection-kind marker; require it and check it matches the base type.
      val declaredKind =
        if (baseField.metadata.contains(CollectionKindMetaKey))
          Some(baseField.metadata.getString(CollectionKindMetaKey))
        else None
      require(
        declaredKind.isDefined,
        s"Parquet restore: column '$name' has array per-element metadata sidecars but no migrator " +
          s"collection-kind marker ('$CollectionKindMetaKey'). This Parquet was not produced by a " +
          "'preserveCollectionTimestamps' export (or was hand-crafted). Refusing to replay it as a " +
          "per-element collection, because an array column cannot be safely distinguished as a CQL " +
          "set vs list without it."
      )
      declaredKind.foreach { kind =>
        val kindMatchesType = (kind, baseField.dataType) match {
          case (CollectionKindMap, _: MapType)   => true
          case (CollectionKindSet, _: ArrayType) => true
          case _                                 => false
        }
        require(
          kindMatchesType,
          s"Parquet restore: column '$name' is marked as CQL '$kind' but its Spark type is " +
            s"${baseField.dataType} (expected ${if (kind == CollectionKindMap) "map" else "array"}). " +
            "Refusing to restore inconsistent per-element collection metadata."
        )
      }
      val ttlType = df.schema(ttlOrd).dataType
      val wtType = df.schema(wtOrd).dataType
      require(
        isPerElementCollectionMetaType(wtType),
        s"Parquet restore: column '$name' has an array '${name}_ttl' sidecar (per-element " +
          s"collection metadata) but its '${name}_writetime' sidecar is $wtType, not an array. The " +
          "TTL and WRITETIME sidecars must both be arrays. Refusing to restore malformed metadata."
      )
      val (ttlElem, wtElem) = (ttlType, wtType) match {
        case (t: ArrayType, w: ArrayType) => (t.elementType, w.elementType)
        case _                            => (ttlType, wtType)
      }
      require(
        ttlElem == IntegerType || ttlElem == LongType,
        s"Parquet restore: '${name}_ttl' array elements are $ttlElem; expected integer/long TTLs."
      )
      require(
        wtElem == IntegerType || wtElem == LongType,
        s"Parquet restore: '${name}_writetime' array elements are $wtElem; expected long WRITETIMEs."
      )
      df.schema(colOrd).dataType match {
        case m: MapType =>
          require(
            sparkKeyOrdering(m.keyType).isDefined,
            s"Parquet restore: map column '$name' has key type ${m.keyType}, which has no supported " +
              "element ordering; per-element metadata cannot be realigned. Refusing to restore."
          )
        case a: ArrayType =>
          require(
            sparkKeyOrdering(a.elementType).isDefined,
            s"Parquet restore: collection column '$name' has element type ${a.elementType}, which " +
              "has no supported ordering; per-element metadata cannot be realigned. Refusing to " +
              "restore."
          )
        case other =>
          throw new IllegalStateException(
            s"Parquet restore: column '$name' has array per-element metadata sidecars but its own " +
              s"type is $other, not a collection (map/list/set). Refusing to restore malformed data."
          )
      }
    }

    val baseRegularKeyOrdinals =
      regularKeyOrdinals.filterNot { case (name, _) => perElementNames.contains(name) }
    val baseOrigSchema =
      StructType(origSchema.fields.filterNot(f => perElementNames.contains(f.name)))

    // Per-element WRITETIME/TTL sidecar ordinals, used to stamp the collection-only base marker row
    // consistently with the collection-append passes (F11 for writetime, C1 for TTL).
    val perElementWritetimeOrdinals =
      perElementNames.toSeq.map(name => regularKeyOrdinals(name)._3)
    val perElementTtlOrdinals =
      perElementNames.toSeq.map(name => regularKeyOrdinals(name)._2)

    val broadcastPrimaryKeyOrdinals = spark.sparkContext.broadcast(primaryKeyOrdinals)
    val broadcastRegularKeyOrdinals = spark.sparkContext.broadcast(baseRegularKeyOrdinals)
    val broadcastSchema = spark.sparkContext.broadcast(baseOrigSchema)
    val broadcastPerElementWtOrdinals = spark.sparkContext.broadcast(perElementWritetimeOrdinals)
    val broadcastPerElementTtlOrdinals = spark.sparkContext.broadcast(perElementTtlOrdinals)
    val finalSchema = StructType(
      baseOrigSchema.fields ++
        Seq(StructField("ttl", IntegerType, true), StructField("writetime", LongType, true))
    )

    if (perElementNames.nonEmpty)
      log.info(
        s"Parquet restore: per-element collection columns detected " +
          s"(${perElementNames.toSeq.sorted.mkString(", ")}); they will be replayed as " +
          "collection-append passes after the base write."
      )
    log.info("Base schema after explosion from per-column metadata:")
    log.info(finalSchema.treeString)

    val explodedRdd = df.rdd.flatMap { row =>
      explodeBaseRow(
        row,
        broadcastSchema.value,
        broadcastPrimaryKeyOrdinals.value,
        broadcastRegularKeyOrdinals.value,
        broadcastPerElementWtOrdinals.value,
        broadcastPerElementTtlOrdinals.value
      )
    }

    val collectionAppendWrites =
      if (perElementNames.isEmpty) Nil
      else
        buildCollectionAppendWritesFromSchema(
          spark,
          df,
          origSchema,
          primaryKeyOrdinals,
          regularKeyOrdinals,
          perElementNames
        )

    (explodedRdd, finalSchema, timestampColumns, collectionAppendWrites)
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

    val nonFrozen = nonFrozenCollectionColumns(tableDef)

    // Choose how to read per-element collection metadata: the Cassandra 5.0 array form or the
    // ScyllaDB 2026.2+ subscript form. Only relevant when per-element preservation is on AND the
    // table actually has non-frozen collections.
    val metadataReadStrategy =
      if (preserveTimes && source.preserveCollectionTimestamps && nonFrozen.nonEmpty)
        detectMetadataReadStrategy(connector, source.keyspace, source.table, nonFrozen)
      else ArrayMetadataRead

    val rdd = metadataReadStrategy match {
      case SubscriptMetadataRead =>
        log.info(
          "Source exposes per-element collection WRITETIME()/TTL() only via the subscript form " +
            "(ScyllaDB 2026.2+); reading element metadata with per-row point reads."
        )
        readRawRddSubscript(
          spark,
          connector,
          source,
          readConf,
          tableDef,
          selection,
          nonFrozen,
          tokenRangesToSkip
        )

      case ArrayMetadataRead =>
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

        finalCassandraRDD
          .asInstanceOf[RDD[Row]]
          .map { row =>
            Row.fromSeq(row.toSeq.map(v => widenTimestampValue(convertValue(v))))
          }
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

          // F5: with per-element collections, the base explode and each collection-append pass are
          // SEPARATE Spark jobs, all derived from `rawDataframe`. Without caching, the source table
          // is re-read once per pass (1 + K scans for K collection columns), and worse, a source
          // mutated between passes could be observed inconsistently (base row from one snapshot,
          // appended elements from another). Persisting pins a single snapshot read once and shared
          // across passes. Safe w.r.t. savepoints: the write-side TokenRangeAccumulator derives
          // ranges from each row's partition-key token, independent of how the read is
          // materialized. `migrate` unpersists it once all passes finish.
          if (perElementNames.nonEmpty)
            rawDataframe.persist(StorageLevel.MEMORY_AND_DISK)

          // Per-element WRITETIME/TTL sidecar ordinals, used to stamp the collection-only base
          // marker row consistently with the collection-append passes (F11 writetime, C1 TTL).
          val perElementWritetimeOrdinals =
            perElementNames.toSeq.flatMap(name => regularKeyOrdinals.get(name).map(_._3))
          val perElementTtlOrdinals =
            perElementNames.toSeq.flatMap(name => regularKeyOrdinals.get(name).map(_._2))

          val broadcastPrimaryKeyOrdinals = spark.sparkContext.broadcast(primaryKeyOrdinals)
          val broadcastRegularKeyOrdinals = spark.sparkContext.broadcast(baseRegularKeyOrdinals)
          val broadcastSchema = spark.sparkContext.broadcast(baseOrigSchema)
          val broadcastPerElementWtOrdinals =
            spark.sparkContext.broadcast(perElementWritetimeOrdinals)
          val broadcastPerElementTtlOrdinals =
            spark.sparkContext.broadcast(perElementTtlOrdinals)
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
              broadcastRegularKeyOrdinals.value,
              broadcastPerElementWtOrdinals.value,
              broadcastPerElementTtlOrdinals.value
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
