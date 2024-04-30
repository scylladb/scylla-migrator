package org.apache.spark.streaming.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}

class V1ToV2RecordProcessor(v1RecordProcessor: IRecordProcessor) extends v2.IRecordProcessor {

  def initialize(initializationInput: InitializationInput): Unit =
    v1RecordProcessor.initialize(initializationInput.getShardId)

  def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    println(s"PROCESSING RECORDS ${processRecordsInput}")
    v1RecordProcessor.processRecords(processRecordsInput.getRecords, processRecordsInput.getCheckpointer)
  }

  def shutdown(shutdownInput: ShutdownInput): Unit =
    v1RecordProcessor.shutdown(shutdownInput.getCheckpointer, shutdownInput.getShutdownReason)

}
