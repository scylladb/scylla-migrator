package com.scylladb.migrator.readers

import org.apache.spark.Partition

private[migrator] case class AerospikePartition(
  override val index: Int,
  partBegin: Int,
  partCount: Int
) extends Partition

/** Connection parameters for Aerospike, serialized as part of the RDD closure. */
private[migrator] case class AerospikeConnectionConfig(
  hosts: List[String],
  port: Int,
  connectTimeoutMs: Option[Int],
  socketTimeoutMs: Option[Int],
  totalTimeoutMs: Option[Int],
  tlsName: Option[String],
  maxConnsPerNode: Option[Int],
  connPoolsPerNode: Option[Int]
) extends Serializable

/** Read parameters for the AerospikeRDD. */
private[migrator] case class AerospikeReadConfig(
  namespace: String,
  set: String,
  splitCount: Int,
  queueSize: Int,
  pollTimeoutSeconds: Int,
  maxScanRetries: Int,
  maxPollRetries: Int
) extends Serializable
