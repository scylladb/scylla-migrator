package com.scylladb.migrator.source.mongodb

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success, Try}

/**
 * Distributed coordination for MongoDB migration using ScyllaDB as the coordination store.
 *
 * This ensures that in a multi-worker Spark cluster:
 * 1. Only one worker performs schema inference and table creation
 * 2. All workers wait for setup to complete before migrating data
 * 3. Oplog start position is captured exactly once and shared across workers
 *
 * Uses ScyllaDB lightweight transactions (LWT) for leader election.
 */
object MongoDBMigrationCoordinator {
  private val log = LoggerFactory.getLogger(getClass)

  private val COORDINATION_KEYSPACE = "scylla_migrator_coordination"
  private val COORDINATION_TABLE = "mongodb_migration_state"

  // Lock TTL in seconds - leader lock expires after this time if not refreshed
  private val LOCK_TTL_SECONDS = 60

  // How long to wait for leader to complete setup
  private val SETUP_TIMEOUT_MS = 300000 // 5 minutes

  /**
   * Migration state stored in coordination table
   */
  case class MigrationState(
      migrationId: String,
      leaderId: String,
      phase: String,
      oplogTimestamp: Option[Long],
      targetKeyspace: String,
      targetTable: String,
      createdAt: Long,
      updatedAt: Long
  )

  object Phase {
    val ACQUIRING_LOCK = "acquiring_lock"
    val INFERRING_SCHEMA = "inferring_schema"
    val CAPTURING_OPLOG = "capturing_oplog"
    val CREATING_TABLE = "creating_table"
    val READY_FOR_MIGRATION = "ready_for_migration"
    val MIGRATING_DATA = "migrating_data"
    val STREAMING_CHANGES = "streaming_changes"
    val COMPLETE = "complete"
    val FAILED = "failed"
  }

  /**
   * Initialize coordination infrastructure
   */
  def initialize(spark: SparkSession): Unit = {
    val connector = CassandraConnector(spark.sparkContext.getConf)

    connector.withSessionDo { session =>
      // Create coordination keyspace if not exists
      session.execute(
        s"""CREATE KEYSPACE IF NOT EXISTS $COORDINATION_KEYSPACE
           |WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
           |AND durable_writes = true""".stripMargin
      )

      // Create coordination table if not exists
      session.execute(
        s"""CREATE TABLE IF NOT EXISTS $COORDINATION_KEYSPACE.$COORDINATION_TABLE (
           |  migration_id text PRIMARY KEY,
           |  leader_id text,
           |  phase text,
           |  oplog_timestamp bigint,
           |  target_keyspace text,
           |  target_table text,
           |  created_at bigint,
           |  updated_at bigint
           |) WITH gc_grace_seconds = 3600""".stripMargin
      )
    }

    log.info("MongoDB migration coordinator initialized")
  }

  /**
   * Try to acquire leader lock for migration setup.
   * Returns true if this worker should perform setup, false if another worker is leader.
   */
  def tryAcquireLeadership(
      spark: SparkSession,
      migrationId: String,
      targetKeyspace: String,
      targetTable: String
  ): Boolean = {
    val connector = CassandraConnector(spark.sparkContext.getConf)
    val workerId = generateWorkerId()
    val now = System.currentTimeMillis()

    connector.withSessionDo { session =>
      // Try to insert new migration record with this worker as leader
      // This uses LWT (IF NOT EXISTS) for atomic leader election
      val insertStmt = session.prepare(
        s"""INSERT INTO $COORDINATION_KEYSPACE.$COORDINATION_TABLE
           |(migration_id, leader_id, phase, target_keyspace, target_table, created_at, updated_at)
           |VALUES (?, ?, ?, ?, ?, ?, ?)
           |IF NOT EXISTS""".stripMargin
      )

      val result = session.execute(insertStmt.bind(
        migrationId,
        workerId,
        Phase.ACQUIRING_LOCK,
        targetKeyspace,
        targetTable,
        java.lang.Long.valueOf(now),
        java.lang.Long.valueOf(now)
      ))

      val applied = result.wasApplied()

      if (applied) {
        log.info(s"Worker $workerId acquired leadership for migration $migrationId")
        true
      } else {
        // Check if we can take over from a stale leader
        val existing = result.one()
        val existingLeader = existing.getString("leader_id")
        val existingUpdatedAt = existing.getLong("updated_at")

        if (now - existingUpdatedAt > LOCK_TTL_SECONDS * 1000) {
          // Leader seems stale, try to take over
          log.info(s"Attempting to take over from stale leader $existingLeader")
          tryTakeOverLeadership(session, migrationId, workerId, existingLeader, now)
        } else {
          log.info(s"Worker $existingLeader is current leader for migration $migrationId")
          false
        }
      }
    }
  }

  /**
   * Try to take over leadership from a stale leader using CAS
   */
  private def tryTakeOverLeadership(
      session: com.datastax.oss.driver.api.core.CqlSession,
      migrationId: String,
      newLeaderId: String,
      oldLeaderId: String,
      now: Long
  ): Boolean = {
    val updateStmt = session.prepare(
      s"""UPDATE $COORDINATION_KEYSPACE.$COORDINATION_TABLE
         |SET leader_id = ?, phase = ?, updated_at = ?
         |WHERE migration_id = ?
         |IF leader_id = ?""".stripMargin
    )

    val result = session.execute(updateStmt.bind(
      newLeaderId,
      Phase.ACQUIRING_LOCK,
      java.lang.Long.valueOf(now),
      migrationId,
      oldLeaderId
    ))

    val applied = result.wasApplied()
    if (applied) {
      log.info(s"Successfully took over leadership from $oldLeaderId")
    }
    applied
  }

  /**
   * Update migration phase (leader only)
   */
  def updatePhase(
      spark: SparkSession,
      migrationId: String,
      phase: String,
      oplogTimestamp: Option[Long] = None
  ): Unit = {
    val connector = CassandraConnector(spark.sparkContext.getConf)
    val now = System.currentTimeMillis()

    connector.withSessionDo { session =>
      val updateStmt = oplogTimestamp match {
        case Some(ts) =>
          session.prepare(
            s"""UPDATE $COORDINATION_KEYSPACE.$COORDINATION_TABLE
               |SET phase = ?, oplog_timestamp = ?, updated_at = ?
               |WHERE migration_id = ?""".stripMargin
          ).bind(phase, java.lang.Long.valueOf(ts), java.lang.Long.valueOf(now), migrationId)

        case None =>
          session.prepare(
            s"""UPDATE $COORDINATION_KEYSPACE.$COORDINATION_TABLE
               |SET phase = ?, updated_at = ?
               |WHERE migration_id = ?""".stripMargin
          ).bind(phase, java.lang.Long.valueOf(now), migrationId)
      }

      session.execute(updateStmt)
      log.info(s"Migration $migrationId updated to phase: $phase")
    }
  }

  /**
   * Wait for migration to reach a specific phase (for non-leader workers)
   */
  def waitForPhase(
      spark: SparkSession,
      migrationId: String,
      targetPhase: String
  ): Option[MigrationState] = {
    val connector = CassandraConnector(spark.sparkContext.getConf)
    val startTime = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTime < SETUP_TIMEOUT_MS) {
      val state = getState(connector, migrationId)

      state match {
        case Some(s) if s.phase == targetPhase || s.phase == Phase.READY_FOR_MIGRATION =>
          log.info(s"Migration reached target phase: ${s.phase}")
          return state

        case Some(s) if s.phase == Phase.FAILED =>
          throw new RuntimeException(s"Migration $migrationId failed during setup")

        case Some(s) =>
          log.debug(s"Waiting for phase $targetPhase, current: ${s.phase}")
          Thread.sleep(1000)

        case None =>
          log.warn(s"Migration $migrationId not found, waiting...")
          Thread.sleep(1000)
      }
    }

    throw new RuntimeException(s"Timeout waiting for migration $migrationId to reach phase $targetPhase")
  }

  /**
   * Get current migration state
   */
  def getState(connector: CassandraConnector, migrationId: String): Option[MigrationState] = {
    connector.withSessionDo { session =>
      val selectStmt = session.prepare(
        s"""SELECT migration_id, leader_id, phase, oplog_timestamp,
           |       target_keyspace, target_table, created_at, updated_at
           |FROM $COORDINATION_KEYSPACE.$COORDINATION_TABLE
           |WHERE migration_id = ?""".stripMargin
      )

      val result = session.execute(selectStmt.bind(migrationId))
      val row = result.one()

      if (row != null) {
        Some(MigrationState(
          migrationId = row.getString("migration_id"),
          leaderId = row.getString("leader_id"),
          phase = row.getString("phase"),
          oplogTimestamp = Option(row.get("oplog_timestamp", classOf[java.lang.Long])).map(_.longValue()),
          targetKeyspace = row.getString("target_keyspace"),
          targetTable = row.getString("target_table"),
          createdAt = row.getLong("created_at"),
          updatedAt = row.getLong("updated_at")
        ))
      } else {
        None
      }
    }
  }

  /**
   * Generate migration ID from source and target info
   */
  def generateMigrationId(
      mongoDatabase: String,
      mongoCollection: String,
      scyllaKeyspace: String,
      scyllaTable: String
  ): String = {
    s"mongodb_${mongoDatabase}_${mongoCollection}_to_${scyllaKeyspace}_${scyllaTable}"
  }

  /**
   * Generate unique worker ID
   */
  private def generateWorkerId(): String = {
    val hostname = Try(java.net.InetAddress.getLocalHost.getHostName).getOrElse("unknown")
    val uuid = UUID.randomUUID().toString.take(8)
    s"$hostname-$uuid"
  }

  /**
   * Clean up coordination state after successful migration
   */
  def cleanup(spark: SparkSession, migrationId: String): Unit = {
    val connector = CassandraConnector(spark.sparkContext.getConf)

    connector.withSessionDo { session =>
      val deleteStmt = session.prepare(
        s"DELETE FROM $COORDINATION_KEYSPACE.$COORDINATION_TABLE WHERE migration_id = ?"
      )
      session.execute(deleteStmt.bind(migrationId))
      log.info(s"Cleaned up coordination state for migration $migrationId")
    }
  }
}
