package com.scylladb.migrator.validation.core

import com.scylladb.migrator.validation.RowComparisonFailure
import org.apache.spark.sql.DataFrame

object ExtraRowDetector {

  def collectExtraTargetFailureSample(
    sourceKeys: DataFrame,
    targetKeys: DataFrame,
    primaryKeyColumns: Seq[String],
    failuresToFetch: Int
  ): List[RowComparisonFailure] =
    if (failuresToFetch <= 0) Nil
    else
      targetKeys
        .join(sourceKeys.distinct(), primaryKeyColumns, "left_anti")
        .rdd
        .map { row =>
          val targetRepr =
            primaryKeyColumns
              .map(pk => s"$pk=${row.getAs[Any](pk)}")
              .mkString(", ")
          RowComparisonFailure(
            targetRepr,
            None,
            List(RowComparisonFailure.Item.ExtraTargetRow)
          )
        }
        .take(failuresToFetch)
        .toList
}
