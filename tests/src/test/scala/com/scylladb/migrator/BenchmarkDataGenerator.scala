package com.scylladb.migrator

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder

object BenchmarkDataGenerator {

  /** Create a simple flat table: (id TEXT PK, col1 TEXT, col2 INT, col3 BIGINT) */
  def createSimpleTable(session: CqlSession, keyspace: String, table: String): Unit = {
    val dropStmt = SchemaBuilder.dropTable(keyspace, table).ifExists().build()
    session.execute(dropStmt)
    val createStmt = SchemaBuilder
      .createTable(keyspace, table)
      .withPartitionKey("id", DataTypes.TEXT)
      .withColumn("col1", DataTypes.TEXT)
      .withColumn("col2", DataTypes.INT)
      .withColumn("col3", DataTypes.BIGINT)
      .build()
    session.execute(createStmt)
  }

  /** Insert rows using batched prepared statements for throughput.
    *
    * @param session
    *   CQL session
    * @param keyspace
    *   target keyspace
    * @param table
    *   target table name
    * @param rowCount
    *   number of rows to insert
    * @param batchSize
    *   number of rows per unlogged batch
    */
  def insertSimpleRows(
    session: CqlSession,
    keyspace: String,
    table: String,
    rowCount: Int,
    batchSize: Int = 100
  ): Unit = {
    val prepared = session.prepare(
      s"INSERT INTO $keyspace.$table (id, col1, col2, col3) VALUES (?, ?, ?, ?)"
    )

    val batches = (0 until rowCount).grouped(batchSize)
    for (batch <- batches) {
      val batchStmt =
        com.datastax.oss.driver.api.core.cql.BatchStatement
          .newInstance(com.datastax.oss.driver.api.core.cql.BatchType.UNLOGGED)

      val stmts = batch.map { i =>
        prepared
          .bind()
          .setString(0, s"id-$i")
          .setString(1, s"value-$i")
          .setInt(2, i)
          .setLong(3, i.toLong * 1000L)
      }

      val finalBatch = stmts.foldLeft(batchStmt)((b, s) => b.add(s))
      session.execute(finalBatch)
    }
  }
}
