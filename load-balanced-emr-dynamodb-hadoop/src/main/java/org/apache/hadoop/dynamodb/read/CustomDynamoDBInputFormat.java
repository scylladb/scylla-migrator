/**
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.dynamodb.read;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.preader.DynamoDBRecordReaderContext;
import org.apache.hadoop.dynamodb.split.DynamoDBSplitGenerator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * This abstract class has a subclass DynamoDBInputFormat that returns a RecordReader that assumes
 * the mapper is taking in key-value pairs in {@code &lt;Text, DynamoDBItemWritable&gt;} format.
 * A new subclass should be created if a different RecordReader is desired.
 */
public class CustomDynamoDBInputFormat implements InputFormat<Text, DynamoDBItemWritable> {

  private static final int MIN_READ_THROUGHPUT_PER_MAP = 100;
  private static final Log log = LogFactory.getLog(CustomDynamoDBInputFormat.class);

  @Override
  public InputSplit[] getSplits(JobConf conf, int desiredSplits) throws IOException {

    double readPercentage = Double.parseDouble(conf.get(DynamoDBConstants
        .THROUGHPUT_READ_PERCENT, DynamoDBConstants.DEFAULT_THROUGHPUT_PERCENTAGE));
    if (readPercentage <= 0) {
      throw new RuntimeException("Invalid read percentage: " + readPercentage);
    }
    log.info("Read percentage: " + readPercentage);

    double maxReadThroughputAllocated = ((double) conf.getLong(DynamoDBConstants.READ_THROUGHPUT,
        1));
    double maxWriteThroughputAllocated = ((double) conf.getLong(DynamoDBConstants
        .WRITE_THROUGHPUT, 1));

    if (maxReadThroughputAllocated < 1.0) {
      throw new RuntimeException("Read throughput should not be less than 1. Read throughput "
          + "percent: " + maxReadThroughputAllocated);
    }

    long tableSizeBytes = conf.getLong(DynamoDBConstants.TABLE_SIZE_BYTES, 1);
    int numSegments = getNumSegments((int) maxReadThroughputAllocated, (int)
        maxWriteThroughputAllocated, tableSizeBytes, conf);
    int numMappers = numSegments;

    log.info("Using " + numSegments + " segments across " + numMappers + " mappers");

    return getSplitGenerator().generateSplits(numMappers, numSegments, conf);
  }

  @Override
  public RecordReader<Text, DynamoDBItemWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    DynamoDBRecordReaderContext context = buildDynamoDBRecordReaderContext(split, job, reporter);
    return new DefaultDynamoDBRecordReader(context);
  }

  protected DynamoDBRecordReaderContext buildDynamoDBRecordReaderContext(InputSplit split,
      JobConf conf, Reporter reporter) {
    DynamoDBRecordReaderContext context = new DynamoDBRecordReaderContext();

    context.setConf(conf);
    context.setSplit(split);
    context.setClient(new DynamoDBClient(conf));
    context.setAverageItemSize(conf.getFloat(DynamoDBConstants.AVG_ITEM_SIZE, 0.0f));
    context.setReporter(reporter);

    return context;
  }

  protected int getNumSegments(int tableNormalizedReadThroughput, int
      tableNormalizedWriteThroughput, long currentTableSizeBytes, JobConf conf) throws IOException {
    // Check for segment count overrides
    int numSegments = conf.getInt(DynamoDBConstants.SCAN_SEGMENTS, -1);
    if (numSegments >= 1) {
      numSegments = Math.min(numSegments, DynamoDBConstants.MAX_SCAN_SEGMENTS);
      numSegments = Math.max(numSegments, DynamoDBConstants.MIN_SCAN_SEGMENTS);

      log.info("Using number of segments configured using " + DynamoDBConstants.SCAN_SEGMENTS
          + ": " + numSegments);
      return numSegments;
    }

    // Segments for size
    int numSegmentsForSize = (int) (currentTableSizeBytes / DynamoDBConstants
        .MAX_BYTES_PER_SEGMENT);
    log.info("Would use " + numSegmentsForSize + " segments for size");

    // Segments for total throughput
    int numSegmentsForThroughput = (int) (tableNormalizedReadThroughput / DynamoDBConstants
        .MIN_IO_PER_SEGMENT);
    log.info("Would use " + numSegmentsForThroughput + " segments for throughput");

    // Take the larger
    numSegments = Math.max(numSegmentsForSize, numSegmentsForThroughput);

    // Fit to bounds
    numSegments = Math.min(numSegments, DynamoDBConstants.MAX_SCAN_SEGMENTS);
    numSegments = Math.max(numSegments, DynamoDBConstants.MIN_SCAN_SEGMENTS);

    log.info("Using computed number of segments: " + numSegments);
    return numSegments;
  }

  protected int getNumMappers(int maxClusterMapTasks, int configuredReadThroughput, JobConf conf)
      throws IOException {
    log.info("Max number of cluster map tasks: " + maxClusterMapTasks);
    log.info("Configured read throughput: " + configuredReadThroughput);

    int numMappers = maxClusterMapTasks;

    // Don't use an excessive number of mappers for a small scan job
    int maxMapTasksForThroughput = configuredReadThroughput / MIN_READ_THROUGHPUT_PER_MAP;
    if (maxMapTasksForThroughput < maxClusterMapTasks) {
      numMappers = maxMapTasksForThroughput;
    }

    // Don't need more mappers than max possible scan segments
    int maxSplits = Math.min(DynamoDBConstants.MAX_SCAN_SEGMENTS, conf.getInt(DynamoDBConstants
        .MAX_MAP_TASKS, DynamoDBConstants.MAX_SCAN_SEGMENTS));
    if (numMappers > maxSplits) {
      log.info("Max number of splits: " + maxSplits);
      numMappers = maxSplits;
    }

    numMappers = Math.max(numMappers, DynamoDBConstants.MIN_SCAN_SEGMENTS);

    log.info("Calculated to use " + numMappers + " mappers");
    return numMappers;
  }

  protected DynamoDBSplitGenerator getSplitGenerator() {
    return new DynamoDBSplitGenerator();
  }

}
