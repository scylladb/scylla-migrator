package com.scylladb.migrator

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder

import java.util.concurrent.{ CompletionException, CompletionStage }
import scala.collection.mutable

object BenchmarkDataGenerator {

  /** Sample indices for spot-checking benchmark rows: first, 25%, 50%, last.
    * Deduplicates indices when rowCount is small (< 4).
    */
  def sampleIndices(rowCount: Int): Seq[Int] =
    Seq(0, rowCount / 4, rowCount / 2, rowCount - 1).distinct.filter(_ >= 0)

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

  /** Insert rows using batched prepared statements with async execution for throughput.
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
    * @param maxConcurrent
    *   maximum number of in-flight async batch requests
    */
  def insertSimpleRows(
    session: CqlSession,
    keyspace: String,
    table: String,
    rowCount: Int,
    batchSize: Int = 100,
    maxConcurrent: Int = 64
  ): Unit = {
    val prepared = session.prepare(
      s"INSERT INTO $keyspace.$table (id, col1, col2, col3) VALUES (?, ?, ?, ?)"
    )

    val inflight = mutable.Queue.empty[CompletionStage[_]]

    def drainOne(): Unit = {
      val future = inflight.dequeue().toCompletableFuture
      try future.join()
      catch {
        case e: CompletionException =>
          // Drain remaining futures to avoid leaking resources, then rethrow
          inflight.foreach { f =>
            try f.toCompletableFuture.join()
            catch { case _: Exception => () }
          }
          inflight.clear()
          throw e
      }
    }

    for (batch <- (0 until rowCount).grouped(batchSize)) {
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
      inflight.enqueue(session.executeAsync(finalBatch))

      // Drain oldest futures when we reach the concurrency limit
      while (inflight.size >= maxConcurrent)
        drainOne()
    }

    // Wait for remaining in-flight requests
    while (inflight.nonEmpty)
      drainOne()
  }
}
