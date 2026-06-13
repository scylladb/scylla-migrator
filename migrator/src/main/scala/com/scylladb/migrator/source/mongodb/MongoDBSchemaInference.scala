package com.scylladb.migrator.source.mongodb

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.scylladb.migrator.config.MongoDBSourceSettings
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.bson.{BsonDocument, BsonType, BsonValue}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Infers ScyllaDB schema from MongoDB collection data.
 *
 * MongoDB is schema-less, so we sample documents to understand the data shape
 * and create an appropriate ScyllaDB schema with proper partition and clustering keys.
 */
object MongoDBSchemaInference {
  private val log = LoggerFactory.getLogger(getClass)

  /**
   * Represents a field discovered in MongoDB documents
   */
  case class InferredField(
      name: String,
      mongoType: BsonType,
      sparkType: DataType,
      cqlType: String,
      isNullable: Boolean,
      occurrenceCount: Long,
      distinctValueEstimate: Long
  )

  /**
   * Represents the inferred schema for a MongoDB collection
   */
  case class InferredSchema(
      fields: Seq[InferredField],
      partitionKey: Seq[String],
      clusteringKeys: Seq[String],
      totalDocuments: Long
  ) {

    /**
     * Generate CQL CREATE TABLE statement
     */
    def toCQL(keyspace: String, table: String): String = {
      val columnDefs = fields.map { f =>
        s"  ${escapeColumnName(f.name)} ${f.cqlType}"
      }.mkString(",\n")

      val pkDef = if (clusteringKeys.isEmpty) {
        s"(${partitionKey.map(escapeColumnName).mkString(", ")})"
      } else {
        s"((${partitionKey.map(escapeColumnName).mkString(", ")}), ${clusteringKeys.map(escapeColumnName).mkString(", ")})"
      }

      s"""CREATE TABLE IF NOT EXISTS $keyspace.$table (
         |$columnDefs,
         |  PRIMARY KEY $pkDef
         |) WITH gc_grace_seconds = 864000
         |  AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'};""".stripMargin
    }

    private def escapeColumnName(name: String): String = {
      // Escape MongoDB field names that are not valid CQL identifiers
      val cleaned = name
        .replace("$", "_dollar_")
        .replace(".", "_dot_")
        .replaceAll("[^a-zA-Z0-9_]", "_")

      if (cleaned.matches("^[0-9].*") || CQLReservedWords.contains(cleaned.toUpperCase)) {
        s""""$cleaned""""
      } else {
        cleaned
      }
    }
  }

  // CQL reserved words that need quoting
  private val CQLReservedWords = Set(
    "ADD", "ALLOW", "ALTER", "AND", "ANY", "APPLY", "ASC", "AUTHORIZE",
    "BATCH", "BEGIN", "BY", "COLUMNFAMILY", "CREATE", "DELETE", "DESC",
    "DROP", "EACH_QUORUM", "ENTRIES", "FROM", "FULL", "GRANT", "IF",
    "IN", "INDEX", "INET", "INFINITY", "INSERT", "INTO", "KEY", "KEYSPACE",
    "KEYSPACES", "LIMIT", "LOCAL_ONE", "LOCAL_QUORUM", "MODIFY", "NAN",
    "NORECURSIVE", "NOT", "NULL", "OF", "ON", "ONE", "ORDER", "PASSWORD",
    "PERMISSION", "PERMISSIONS", "PRIMARY", "QUORUM", "RENAME", "REVOKE",
    "SCHEMA", "SELECT", "SET", "TABLE", "THREE", "TOKEN", "TRUNCATE",
    "TWO", "UNLOGGED", "UPDATE", "USE", "USING", "WHERE", "WITH"
  )

  /**
   * Infer schema from MongoDB collection by sampling documents
   */
  def inferSchema(
      spark: SparkSession,
      settings: MongoDBSourceSettings
  ): InferredSchema = {
    log.info(s"Inferring schema from MongoDB collection ${settings.database}.${settings.collection}")

    val readConfig = ReadConfig(Map(
      "uri" -> settings.connectionUri,
      "database" -> settings.database,
      "collection" -> settings.collection,
      "readPreference.name" -> settings.readPreference
    ))

    // Sample documents for schema inference
    val rdd = MongoSpark.load(spark.sparkContext, readConfig)
    val sampleDocs = rdd.take(settings.sampleSize)
    val totalDocs = rdd.count()

    log.info(s"Sampled ${sampleDocs.length} documents out of $totalDocs total")

    // Analyze field types and cardinality
    val fieldAnalysis = analyzeFields(sampleDocs)

    // Convert to InferredFields
    val fields = fieldAnalysis.map { case (name, analysis) =>
      InferredField(
        name = name,
        mongoType = analysis.mostCommonType,
        sparkType = bsonTypeToSparkType(analysis.mostCommonType, analysis.nestedTypes),
        cqlType = bsonTypeToCQLType(analysis.mostCommonType, analysis.nestedTypes),
        isNullable = analysis.nullCount > 0,
        occurrenceCount = analysis.count,
        distinctValueEstimate = analysis.distinctValues.size
      )
    }.toSeq.sortBy(_.name)

    // Determine partition and clustering keys
    val (partitionKey, clusteringKeys) = determineKeys(fields, settings)

    InferredSchema(
      fields = fields,
      partitionKey = partitionKey,
      clusteringKeys = clusteringKeys,
      totalDocuments = totalDocs
    )
  }

  private case class FieldAnalysis(
      count: Long,
      nullCount: Long,
      types: mutable.Map[BsonType, Long],
      distinctValues: mutable.Set[Int], // Store hash codes for memory efficiency
      nestedTypes: mutable.Map[String, BsonType]
  ) {
    def mostCommonType: BsonType = {
      types.maxBy(_._2)._1
    }
  }

  private def analyzeFields(docs: Array[org.bson.Document]): Map[String, FieldAnalysis] = {
    val analysis = mutable.Map[String, FieldAnalysis]()

    docs.foreach { doc =>
      analyzeDocument(doc, "", analysis)
    }

    analysis.toMap
  }

  private def analyzeDocument(
      doc: org.bson.Document,
      prefix: String,
      analysis: mutable.Map[String, FieldAnalysis]
  ): Unit = {
    doc.keySet().asScala.foreach { key =>
      val fullName = if (prefix.isEmpty) key else s"${prefix}_$key"
      val value = doc.get(key)
      val bsonValue = doc.toBsonDocument.get(key)

      val fieldAnalysis = analysis.getOrElseUpdate(fullName, FieldAnalysis(
        count = 0,
        nullCount = 0,
        types = mutable.Map.empty,
        distinctValues = mutable.Set.empty,
        nestedTypes = mutable.Map.empty
      ))

      fieldAnalysis.count += 1

      if (value == null || bsonValue == null || bsonValue.isNull) {
        fieldAnalysis.nullCount += 1
        fieldAnalysis.types.update(BsonType.NULL, fieldAnalysis.types.getOrElse(BsonType.NULL, 0L) + 1)
      } else {
        val bsonType = bsonValue.getBsonType
        fieldAnalysis.types.update(bsonType, fieldAnalysis.types.getOrElse(bsonType, 0L) + 1)

        // Track distinct values (using hash code for memory efficiency)
        if (value != null) {
          fieldAnalysis.distinctValues.add(value.hashCode())
        }

        // For arrays, track element type
        if (bsonType == BsonType.ARRAY) {
          val arr = bsonValue.asArray()
          if (!arr.isEmpty) {
            val elemType = arr.get(0).getBsonType
            fieldAnalysis.nestedTypes.update("element", elemType)
          }
        }

        // For embedded documents, recurse
        if (bsonType == BsonType.DOCUMENT && value.isInstanceOf[org.bson.Document]) {
          analyzeDocument(value.asInstanceOf[org.bson.Document], fullName, analysis)
        }
      }
    }
  }

  private def bsonTypeToSparkType(bsonType: BsonType, nested: mutable.Map[String, BsonType]): DataType = {
    bsonType match {
      case BsonType.STRING => StringType
      case BsonType.INT32 => IntegerType
      case BsonType.INT64 => LongType
      case BsonType.DOUBLE => DoubleType
      case BsonType.DECIMAL128 => DecimalType(38, 18)
      case BsonType.BOOLEAN => BooleanType
      case BsonType.DATE_TIME => TimestampType
      case BsonType.TIMESTAMP => TimestampType
      case BsonType.OBJECT_ID => StringType
      case BsonType.BINARY => BinaryType
      case BsonType.ARRAY =>
        val elemType = nested.get("element").map(bsonTypeToSparkType(_, mutable.Map.empty)).getOrElse(StringType)
        ArrayType(elemType)
      case BsonType.DOCUMENT => StringType // Store as JSON string
      case BsonType.NULL => StringType
      case _ => StringType
    }
  }

  private def bsonTypeToCQLType(bsonType: BsonType, nested: mutable.Map[String, BsonType]): String = {
    bsonType match {
      case BsonType.STRING => "text"
      case BsonType.INT32 => "int"
      case BsonType.INT64 => "bigint"
      case BsonType.DOUBLE => "double"
      case BsonType.DECIMAL128 => "decimal"
      case BsonType.BOOLEAN => "boolean"
      case BsonType.DATE_TIME => "timestamp"
      case BsonType.TIMESTAMP => "timestamp"
      case BsonType.OBJECT_ID => "text" // Store ObjectId as string
      case BsonType.BINARY => "blob"
      case BsonType.ARRAY =>
        val elemType = nested.get("element").map(t => bsonTypeToCQLType(t, mutable.Map.empty)).getOrElse("text")
        s"list<$elemType>"
      case BsonType.DOCUMENT => "text" // Store as JSON
      case BsonType.NULL => "text"
      case _ => "text"
    }
  }

  /**
   * Determine the best partition and clustering keys based on field analysis.
   * 
   * Strategy:
   * 1. If user specified keys, use those
   * 2. If _id exists, use it as partition key
   * 3. Look for high-cardinality fields that appear in all documents
   * 4. Consider field naming patterns (id, key, code, etc.)
   */
  private def determineKeys(
      fields: Seq[InferredField],
      settings: MongoDBSourceSettings
  ): (Seq[String], Seq[String]) = {

    // If user specified keys, use them
    val userPartitionKey = settings.partitionKeyField.map(Seq(_)).getOrElse(Seq.empty)
    val userClusteringKeys = settings.clusteringKeyFields.getOrElse(Seq.empty)

    if (userPartitionKey.nonEmpty) {
      log.info(s"Using user-specified partition key: ${userPartitionKey.mkString(", ")}")
      return (userPartitionKey, userClusteringKeys)
    }

    // Look for _id field (MongoDB's default primary key)
    val idField = fields.find(_.name == "_id")
    if (idField.isDefined) {
      log.info("Using MongoDB _id as partition key")
      return (Seq("_id"), Seq.empty)
    }

    // Find good partition key candidates:
    // - High cardinality
    // - Appears in all/most documents  
    // - Named like an identifier
    val idPatterns = Seq("id", "key", "code", "uuid", "guid", "pk")
    val candidates = fields
      .filter(f => f.occurrenceCount > 0)
      .filter(f => !f.cqlType.startsWith("list<") && f.cqlType != "blob")
      .sortBy { f =>
        val cardinalityScore = f.distinctValueEstimate.toDouble / f.occurrenceCount
        val nameScore = if (idPatterns.exists(p => f.name.toLowerCase.contains(p))) 2.0 else 1.0
        -(cardinalityScore * nameScore)
      }

    if (candidates.nonEmpty) {
      val pk = candidates.head.name
      log.info(s"Auto-selected partition key: $pk (cardinality estimate: ${candidates.head.distinctValueEstimate})")

      // Look for potential clustering key (timestamp or sequential field)
      val clusteringCandidates = fields
        .filter(f => f.name != pk)
        .filter(f => f.cqlType == "timestamp" || f.cqlType == "bigint" || f.name.toLowerCase.contains("time") || f.name.toLowerCase.contains("seq"))

      val ck = clusteringCandidates.headOption.map(_.name).toSeq
      if (ck.nonEmpty) {
        log.info(s"Auto-selected clustering key: ${ck.mkString(", ")}")
      }

      (Seq(pk), ck)
    } else {
      // Fallback: generate a synthetic key
      log.warn("Could not determine suitable partition key - will use synthetic row_id")
      (Seq("_row_id"), Seq.empty)
    }
  }
}
