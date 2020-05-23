package com.scylladb.migrator

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model.{ CreateTableRequest, ProvisionedThroughput }
import com.scylladb.migrator.config.AWSCredentials
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.{ DynamoDBClient, DynamoDBConstants, DynamoDBItemWritable }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DynamoDBMigrator {
  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    val migratorConfig =
      DynamoDBMigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))
    log.info(s"Loaded config: ${migratorConfig}")

    val sc = spark.sparkContext
    val tableToRead = migratorConfig.source.table
    val jobConf = getSourceDynamoDbJobConf(
      sc,
      tableToRead,
      migratorConfig.source.read_throughput,
      migratorConfig.source.throughput_read_percent,
      migratorConfig.source.max_map_tasks,
      migratorConfig.source.scan_segments
    )
    val sourceEndpoint = migratorConfig.source.hostURL
      .getOrElse("") + ":" + migratorConfig.source.port.getOrElse("")
    if (":" != sourceEndpoint) { //endpoint not needed if region is there
      jobConf.set(DynamoDBConstants.ENDPOINT, sourceEndpoint)
    }
    jobConf.set(DynamoDBConstants.REGION_ID, migratorConfig.source.region)
    val creds: AWSCredentials =
      migratorConfig.source.credentials.getOrElse(AWSCredentials("empty", "empty"))
    val aws_key = creds.accessKey;
    jobConf.set(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF, aws_key)
    val aws_secret = creds.secretKey
    jobConf.set(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF, aws_secret)

    val tableToWrite = migratorConfig.target.table.getOrElse(tableToRead)

    // replicate the schema to destination table
    val sourceClient = new DynamoDBClient(jobConf)
    val sourceTableDesc = sourceClient.describeTable(tableToRead)

    val destEndpoint = migratorConfig.target.hostURL + ":" + migratorConfig.target.port
    val destRegion = migratorConfig.target.region.getOrElse("empty")
    val destcreds: AWSCredentials =
      migratorConfig.target.credentials.getOrElse(AWSCredentials("empty", "empty"))
    val config = new AwsClientBuilder.EndpointConfiguration(destEndpoint, destRegion)
    val credentials = new BasicAWSCredentials(destcreds.accessKey, destcreds.secretKey)
    val credentialsProvider = new AWSStaticCredentialsProvider(credentials)
    val client = AmazonDynamoDBClientBuilder
      .standard()
      .withEndpointConfiguration(config)
      .withCredentials(credentialsProvider)
      .build();
    val dynamoDB = new DynamoDB(client)
    //https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/JavaDocumentAPIWorkingWithTables.html
    val request = new CreateTableRequest()
      .withTableName(tableToWrite)
      .withKeySchema(sourceTableDesc.getKeySchema)
      .withAttributeDefinitions(sourceTableDesc.getAttributeDefinitions)
      .withProvisionedThroughput(new ProvisionedThroughput(
        sourceTableDesc.getProvisionedThroughput.getReadCapacityUnits,
        sourceTableDesc.getProvisionedThroughput.getWriteCapacityUnits))
    val table = dynamoDB.createTable(request)

    //start migration
    val rows = sc.hadoopRDD(
      jobConf,
      classOf[DynamoDBInputFormat],
      classOf[Text],
      classOf[DynamoDBItemWritable])

    val dstJobConf = getDestinationDynamoDbJobConf(
      sc,
      tableToWrite,
      migratorConfig.target.write_throughput,
      migratorConfig.target.throughput_write_percent,
      migratorConfig.target.max_map_tasks,
      migratorConfig.target.scan_segments
    )
    dstJobConf.set(DynamoDBConstants.ENDPOINT, destEndpoint)
    dstJobConf.set(DynamoDBConstants.REGION_ID, destRegion)
    dstJobConf.set(DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF, destcreds.accessKey)
    dstJobConf.set(DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF, destcreds.secretKey)

    rows.saveAsHadoopDataset(dstJobConf)

  }

  def getCommonDynamoDbJobConf(sc: SparkContext) = {
    val jobConf = new JobConf(sc.hadoopConfiguration)
//    jobConf.set("dynamodb.servicename", "dynamodb")
    jobConf
  }

  def getSourceDynamoDbJobConf(sc: SparkContext,
                               tableNameForRead: String,
                               READ_THROUGHPUT: Option[Int] = None,
                               THROUGHPUT_READ_PERCENT: Option[Float] = None,
                               MAX_MAP_TASKS: Option[Int] = None,
                               SCAN_SEGMENTS: Option[Int] = None) = {
    val jobConf = getCommonDynamoDbJobConf(sc);
    jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, tableNameForRead)
    jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
    if (READ_THROUGHPUT != None) {
      jobConf.set(DynamoDBConstants.READ_THROUGHPUT, String.valueOf(READ_THROUGHPUT.get))
    }
    if (THROUGHPUT_READ_PERCENT != None) {
      jobConf.set(
        DynamoDBConstants.THROUGHPUT_READ_PERCENT,
        String.valueOf(THROUGHPUT_READ_PERCENT.get))
    }
    if (MAX_MAP_TASKS != None) {
      jobConf.set(DynamoDBConstants.MAX_MAP_TASKS, String.valueOf(MAX_MAP_TASKS.get))
    }
    if (SCAN_SEGMENTS != None) {
      jobConf.set(DynamoDBConstants.SCAN_SEGMENTS, String.valueOf(SCAN_SEGMENTS.get)) // control split factor
    }
    jobConf
  }

  def getDestinationDynamoDbJobConf(sc: SparkContext,
                                    tableNameForWrite: String,
                                    WRITE_THROUGHPUT: Option[Int] = None,
                                    THROUGHPUT_WRITE_PERCENT: Option[Float] = None,
                                    MAX_MAP_TASKS: Option[Int] = None,
                                    SCAN_SEGMENTS: Option[Int] = None) = {
    val jobConf = getCommonDynamoDbJobConf(sc);
    jobConf.set(DynamoDBConstants.OUTPUT_TABLE_NAME, tableNameForWrite)
    jobConf.set(
      "mapred.output.format.class",
      "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    if (WRITE_THROUGHPUT != None) {
      jobConf.set(DynamoDBConstants.WRITE_THROUGHPUT, String.valueOf(WRITE_THROUGHPUT.get))
    }
    if (THROUGHPUT_WRITE_PERCENT != None) {
      jobConf.set(
        DynamoDBConstants.THROUGHPUT_WRITE_PERCENT,
        String.valueOf(THROUGHPUT_WRITE_PERCENT.get))
    }
    if (MAX_MAP_TASKS != None) {
      jobConf.set(DynamoDBConstants.MAX_MAP_TASKS, String.valueOf(MAX_MAP_TASKS.get))
    }
    if (SCAN_SEGMENTS != None) {
      jobConf.set(DynamoDBConstants.SCAN_SEGMENTS, String.valueOf(SCAN_SEGMENTS.get)) // control split factor
    }
    jobConf
  }

}
