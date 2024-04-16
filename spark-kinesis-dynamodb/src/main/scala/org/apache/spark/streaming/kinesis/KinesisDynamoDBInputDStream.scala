package org.apache.spark.streaming.kinesis

import com.amazonaws.services.kinesis.model.Record
import org.apache.spark.streaming.kinesis.KinesisInputDStream.{DEFAULT_KINESIS_ENDPOINT_URL, DEFAULT_STORAGE_LEVEL}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

/**
  * Override the default behavior of [[KinesisInputDStream]] to create a [[KinesisDynamoDBReceiver]].
  */
class KinesisDynamoDBInputDStream[T: ClassTag](
  ssc: StreamingContext,
  streamName: String,
  regionName: String,
  initialPosition: KinesisInitialPosition,
  checkpointAppName: String,
  messageHandler: Record => T,
  kinesisCreds: SparkAWSCredentials
) extends KinesisInputDStream[T](
      ssc,
      streamName,
      DEFAULT_KINESIS_ENDPOINT_URL,
      regionName,
      initialPosition,
      checkpointAppName,
      ssc.graph.batchDuration,
      DEFAULT_STORAGE_LEVEL,
      messageHandler,
      kinesisCreds,
      None,
      None
    ) {

  override def getReceiver(): Receiver[T] = {
    new KinesisDynamoDBReceiver(
      streamName,
      endpointUrl,
      regionName,
      initialPosition,
      checkpointAppName,
      checkpointInterval,
      DEFAULT_STORAGE_LEVEL,
      messageHandler,
      kinesisCreds,
      dynamoDBCreds,
      cloudWatchCreds
    )
  }

}
