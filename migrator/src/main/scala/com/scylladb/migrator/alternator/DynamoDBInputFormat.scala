package com.scylladb.migrator.alternator
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.dynamodb.DynamoDBConstants
import org.apache.hadoop.mapred.{ InputSplit, JobConf }

/**
  * Specializes the split strategy:
  *    - use as many partitions as the number of scan segments
  *    - by default, create segments that split the data into 128 MB chunks
  */
class DynamoDBInputFormat extends org.apache.hadoop.dynamodb.read.DynamoDBInputFormat {

  private val log = LogFactory.getLog(classOf[DynamoDBInputFormat])

  override def getSplits(conf: JobConf, desiredSplits: Int): Array[InputSplit] = {
    val readPercentage = conf.getDouble(
      DynamoDBConstants.THROUGHPUT_READ_PERCENT,
      DynamoDBConstants.DEFAULT_THROUGHPUT_PERCENTAGE.toDouble)
    if (readPercentage <= 0) {
      sys.error(s"Invalid read percentage: ${readPercentage}")
    }
    log.info(s"Read percentage: ${readPercentage}")
    val maxReadThroughputAllocated = conf.getInt(DynamoDBConstants.READ_THROUGHPUT, 1)
    val maxWriteThroughputAllocated = conf.getInt(DynamoDBConstants.WRITE_THROUGHPUT, 1)
    if (maxReadThroughputAllocated < 1.0) {
      sys.error(
        s"Read throughput should not be less than 1. Read throughput percent: ${maxReadThroughputAllocated}")
    }

    val configuredReadThroughput =
      math.max(math.floor(maxReadThroughputAllocated * readPercentage).intValue(), 1)

    val tableSizeBytes = conf.getLong(DynamoDBConstants.TABLE_SIZE_BYTES, 1)
    val numSegments =
      getNumSegments(maxReadThroughputAllocated, maxWriteThroughputAllocated, tableSizeBytes, conf)

    val numMappers = getNumMappers(numSegments, configuredReadThroughput, conf)

    log.info(s"Using ${numSegments} segments across ${numMappers} mappers")

    getSplitGenerator.generateSplits(numMappers, numSegments, conf)
  }

  override def getNumSegments(tableNormalizedReadThroughput: Int,
                              tableNormalizedWriteThroughput: Int,
                              currentTableSizeBytes: Long,
                              conf: JobConf): Int = {
    // Use configured scan segment if provided
    val configuredScanSegment = conf.getInt(DynamoDBConstants.SCAN_SEGMENTS, -1)
    if (configuredScanSegment > 0) {
      val numSegments =
        math.max(
          math.min(configuredScanSegment, DynamoDBConstants.MAX_SCAN_SEGMENTS),
          DynamoDBConstants.MIN_SCAN_SEGMENTS
        )
      log.info(
        s"Using number of segments configured using ${DynamoDBConstants.SCAN_SEGMENTS}: ${numSegments}")
      numSegments
    } else {
      // split into segments of at most 100 MB each (note: upstream implementation splits into 1 GB segments)
      val numSegmentsForSize = {
        val bytesPerSegment = 100 * 1024 * 1024
        (currentTableSizeBytes.toDouble / bytesPerSegment).ceil.intValue()
      }
      log.info(s"Would use ${numSegmentsForSize} segments for size")

      val numSegmentsForThroughput =
        (tableNormalizedReadThroughput / DynamoDBConstants.MIN_IO_PER_SEGMENT).intValue()
      log.info(s"Would use ${numSegmentsForThroughput} segments for throughput")

      // Take the smallest and fit to bounds
      val numSegments =
        math.max(
          math.min(
            math.min(numSegmentsForSize, numSegmentsForThroughput),
            DynamoDBConstants.MAX_SCAN_SEGMENTS
          ),
          DynamoDBConstants.MIN_SCAN_SEGMENTS
        )
      log.info(s"Using computed number of segments: ${numSegments}")
      numSegments
    }
  }

  // Implementation copied from https://github.com/awslabs/emr-dynamodb-connector/blob/f52cef1ceac75c524f47996802a10f2bd6554c7e/emr-dynamodb-hadoop/src/main/java/org/apache/hadoop/dynamodb/read/AbstractDynamoDBInputFormat.java
  // and adapted to initialize the number of mappers to the number of segments
  private def getNumMappers(numSegments: Int, configuredReadThroughput: Int, conf: JobConf): Int = {
    log.info("Number of segments: " + numSegments)
    log.info("Configured read throughput: " + configuredReadThroughput)

    var numMappers = numSegments

    // Don't use an excessive number of mappers for a small scan job
    val maxMapTasksForThroughput = configuredReadThroughput / 100
    if (maxMapTasksForThroughput < numSegments) numMappers = maxMapTasksForThroughput

    // Don't need more mappers than max possible scan segments
    val maxSplits = Math.min(
      DynamoDBConstants.MAX_SCAN_SEGMENTS,
      conf.getInt(DynamoDBConstants.MAX_MAP_TASKS, DynamoDBConstants.MAX_SCAN_SEGMENTS))
    if (numMappers > maxSplits) {
      log.info("Max number of splits: " + maxSplits)
      numMappers = maxSplits
    }

    numMappers = Math.max(numMappers, DynamoDBConstants.MIN_SCAN_SEGMENTS)

    log.info("Calculated to use " + numMappers + " mappers")
    numMappers
  }

}
