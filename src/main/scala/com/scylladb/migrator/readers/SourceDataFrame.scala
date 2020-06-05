package com.scylladb.migrator.readers

import com.scylladb.migrator.writer.Writer.TimestampColumns
import org.apache.spark.sql.DataFrame

case class SourceDataFrame(dataFrame: DataFrame,
                           timestampColumns: Option[TimestampColumns],
                           savepointsSupported: Boolean)
