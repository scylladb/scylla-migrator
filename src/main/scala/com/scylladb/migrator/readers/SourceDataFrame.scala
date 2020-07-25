package com.scylladb.migrator.readers

import org.apache.spark.sql.DataFrame

case class TimestampColumns(ttl: String, writeTime: String)
case class SourceDataFrame(dataFrame: DataFrame,
                           timestampColumns: Option[TimestampColumns],
                           savepointsSupported: Boolean)
