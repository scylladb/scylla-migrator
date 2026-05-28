package com.scylladb.migrator.validation

import com.datastax.spark.connector.CassandraRow
import com.scylladb.migrator.alternator.DdbValue
import com.scylladb.migrator.validation.core.{
  ComparisonResult,
  NumericComparison,
  NumericTypePolicy
}

import java.time.temporal.ChronoUnit

/** Represents a single row comparison failure found during validation.
  *
  * @param rowRepr
  *   For most failure types this is the source row's PK representation. For [[Item.ExtraTargetRow]]
  *   failures, it holds the target row's PK (since no source row exists); the `toString` method
  *   adjusts the labels accordingly.
  * @param otherRepr
  *   The target row's PK representation, or `None` when the target row is missing (or when the
  *   failure is [[Item.ExtraTargetRow]]).
  * @param items
  *   The list of comparison failure details.
  */
case class RowComparisonFailure(
  rowRepr: String,
  otherRepr: Option[String],
  items: List[RowComparisonFailure.Item]
) {

  override def toString: String = {
    val isExtraTarget = items.contains(RowComparisonFailure.Item.ExtraTargetRow)
    val (srcLabel, tgtLabel) =
      if (isExtraTarget)
        ("<N/A - row only exists in target>", rowRepr)
      else
        (rowRepr, otherRepr.getOrElse("<MISSING>"))
    s"""
       |Row failure:
       |* Source row: ${srcLabel}
       |* Target row: ${tgtLabel}
       |* Failures:
       |${items.map(item => s"  - ${item.description}").mkString("\n")}
     """.stripMargin
  }
}

object RowComparisonFailure {
  sealed abstract class Item(val description: String) extends Serializable
  object Item {
    case object MissingTargetRow extends Item("Missing target row")
    case object ExtraTargetRow extends Item("Extra target row (not present in source)")
    case object MismatchedColumnCount extends Item("Mismatched column count")
    case object MismatchedColumnNames extends Item("Mismatched column names")
    case class DifferingFieldValues(fields: List[String])
        extends Item(s"Differing fields: ${fields.mkString(", ")}")
    case class DifferingTtls(details: List[(String, Long)]) extends Item(s"Differing TTLs: ${details
            .map { case (fieldName, ttlDiff) =>
              s"$fieldName ($ttlDiff millis)"
            }
            .mkString(", ")}")
    case class DifferingWritetimes(details: List[(String, Long)])
        extends Item(s"Differing WRITETIMEs: ${details
            .map { case (fieldName, writeTimeDiff) =>
              s"$fieldName ($writeTimeDiff millis)"
            }
            .mkString(", ")}")
    case class NumericTypeMismatch(fields: List[(String, String, String)])
        extends Item(s"Numeric type mismatches: ${fields
            .map { case (fieldName, srcType, tgtType) =>
              s"$fieldName (source type $srcType vs target type $tgtType)"
            }
            .mkString(", ")}")
  }

  def cassandraRowComparisonFailure(
    left: CassandraRow,
    right: Option[CassandraRow],
    items: List[Item]
  ): RowComparisonFailure =
    RowComparisonFailure(left.toString, right.map(_.toString), items)

  def compareCassandraRows(
    left: CassandraRow,
    right: Option[CassandraRow],
    floatingPointTolerance: Double,
    timestampMsTolerance: Long,
    ttlToleranceMillis: Long,
    writetimeToleranceMillis: Long,
    compareTimestamps: Boolean,
    numericTypePolicy: NumericTypePolicy = NumericTypePolicy.Lenient
  ): Option[RowComparisonFailure] =
    right match {
      case None => Some(cassandraRowComparisonFailure(left, right, List(Item.MissingTargetRow)))
      case Some(right) if left.columnValues.size != right.columnValues.size =>
        Some(cassandraRowComparisonFailure(left, Some(right), List(Item.MismatchedColumnCount)))
      case Some(right) if left.metaData.columnNames != right.metaData.columnNames =>
        Some(cassandraRowComparisonFailure(left, Some(right), List(Item.MismatchedColumnNames)))
      case Some(right) =>
        val names = left.metaData.columnNames

        val leftMap = left.toMap
        val rightMap = right.toMap

        val (differingFieldValues, numericTypeMismatches) =
          names
            .filterNot(name => name.endsWith("_ttl") || name.endsWith("_writetime"))
            .foldLeft(
              (List.empty[String], List.empty[(String, String, String)])
            ) { case ((diffs, mismatches), name) =>
              val leftValue = leftMap.get(name)
              val rightValue = rightMap.get(name)

              (leftValue, rightValue) match {
                case (Some(l: Number), Some(r: Number)) =>
                  NumericComparison.compareWithPolicy(
                    l,
                    r,
                    floatingPointTolerance,
                    numericTypePolicy
                  ) match {
                    case ComparisonResult.Different =>
                      (name :: diffs, mismatches)
                    case ComparisonResult.TypeMismatch(src, tgt) =>
                      (diffs, (name, src, tgt) :: mismatches)
                    case ComparisonResult.Equal =>
                      (diffs, mismatches)
                  }
                case _ =>
                  if (
                    areDifferent(
                      leftValue,
                      rightValue,
                      timestampMsTolerance,
                      floatingPointTolerance,
                      numericTypePolicy
                    )
                  )
                    (name :: diffs, mismatches)
                  else
                    (diffs, mismatches)
              }
            }

        val differingTtls =
          if (!compareTimestamps) Nil
          else
            for {
              name <- names
              if name.endsWith("_ttl")
              leftTtl  = left.getLongOption(name)
              rightTtl = right.getLongOption(name)
              result <- (leftTtl, rightTtl) match {
                          case (Some(l), Some(r)) if math.abs(l - r) > ttlToleranceMillis =>
                            Some(name -> math.abs(l - r))
                          case (Some(l), None)    => Some(name -> l)
                          case (None, Some(r))    => Some(name -> r)
                          case (Some(l), Some(r)) => None
                          case (None, None)       => None
                        }
            } yield result

        // WRITETIME is expressed in microseconds
        val writetimeToleranceMicros = writetimeToleranceMillis * 1000
        val differingWritetimes =
          if (!compareTimestamps) Nil
          else
            for {
              name <- names
              if name.endsWith("_writetime")
              leftWritetime  = left.getLongOption(name)
              rightWritetime = right.getLongOption(name)
              result <- (leftWritetime, rightWritetime) match {
                          case (Some(l), Some(r)) if math.abs(l - r) > writetimeToleranceMicros =>
                            Some(name -> math.abs(l - r))
                          case (Some(l), None)    => Some(name -> l)
                          case (None, Some(r))    => Some(name -> r)
                          case (Some(l), Some(r)) => None
                          case (None, None)       => None
                        }
            } yield result

        if (
          differingFieldValues.isEmpty && numericTypeMismatches.isEmpty && differingTtls.isEmpty && differingWritetimes.isEmpty
        )
          None
        else
          Some(
            cassandraRowComparisonFailure(
              left,
              Some(right),
              (if (differingFieldValues.nonEmpty)
                 List(Item.DifferingFieldValues(differingFieldValues.reverse))
               else Nil) ++
                (if (numericTypeMismatches.nonEmpty)
                   List(Item.NumericTypeMismatch(numericTypeMismatches.reverse))
                 else Nil) ++
                (if (differingTtls.nonEmpty) List(Item.DifferingTtls(differingTtls.toList))
                 else Nil) ++
                (if (differingWritetimes.nonEmpty)
                   List(Item.DifferingWritetimes(differingWritetimes.toList))
                 else Nil)
            )
          )
    }

  def dynamoDBRowComparisonFailure(
    left: collection.Map[String, DdbValue],
    maybeRight: Option[collection.Map[String, DdbValue]],
    items: List[Item]
  ): RowComparisonFailure =
    RowComparisonFailure(left.toString, maybeRight.map(_.toString), items)

  /** @param left
    *   The first item to compare
    * @param maybeRight
    *   The possible second item to compare
    * @param renamedColumn
    *   A function describing how the columns of the first items should be expected to be renamed in
    *   the second item
    * @param floatingPointTolerance
    *   The tolerance to apply when comparing floating point values
    * @return
    *   Some comparison failure if the compared items were different, otherwise `None`.
    */
  def compareDynamoDBRows(
    left: collection.Map[String, DdbValue],
    maybeRight: Option[collection.Map[String, DdbValue]],
    renamedColumn: String => String,
    floatingPointTolerance: Double,
    numericTypePolicy: NumericTypePolicy = NumericTypePolicy.Lenient
  ): Option[RowComparisonFailure] =
    maybeRight match {
      case None => Some(dynamoDBRowComparisonFailure(left, maybeRight, List(Item.MissingTargetRow)))
      case Some(right) if left.keySet.size != right.keySet.size =>
        Some(dynamoDBRowComparisonFailure(left, maybeRight, List(Item.MismatchedColumnCount)))
      case Some(right) if left.keySet.map(renamedColumn) != right.keySet =>
        Some(dynamoDBRowComparisonFailure(left, maybeRight, List(Item.MismatchedColumnNames)))
      case Some(right) =>
        val differingFieldValues =
          for {
            (columnName, leftValue) <- left
            rightValue = right.get(renamedColumn(columnName))
            if areDifferent(
              Some(leftValue),
              rightValue,
              0L, // There is no Timestamp type in DynamoDB
              floatingPointTolerance,
              numericTypePolicy
            )
          } yield columnName
        if (differingFieldValues.isEmpty) None
        else
          Some(
            dynamoDBRowComparisonFailure(
              left,
              maybeRight,
              List(Item.DifferingFieldValues(differingFieldValues.toList))
            )
          )
    }

  /** @param leftValue
    *   First value to compare
    * @param rightValue
    *   Second value to compare
    * @param timestampMsTolerance
    *   Tolerance when comparing instant values
    * @param floatingPointTolerance
    *   Tolerance when comparing floating point values
    * @return
    *   `true` if the `leftValue` is different from the `rightValue`
    */
  private[migrator] def areDifferent(
    leftValue: Option[Any],
    rightValue: Option[Any],
    timestampMsTolerance: Long,
    floatingPointTolerance: Double,
    numericTypePolicy: NumericTypePolicy = NumericTypePolicy.Lenient
  ): Boolean =
    (leftValue, rightValue) match {
      // All timestamp types need to be compared with a configured tolerance
      case (Some(l: java.sql.Timestamp), Some(r: java.sql.Timestamp)) if timestampMsTolerance > 0 =>
        Math.abs(l.getTime - r.getTime) > timestampMsTolerance
      case (Some(l: java.sql.Timestamp), Some(r: java.sql.Timestamp)) =>
        l.getTime != r.getTime || l.getNanos != r.getNanos
      case (Some(l: java.time.Instant), Some(r: java.time.Instant)) if timestampMsTolerance > 0 =>
        Math.abs(r.until(l, ChronoUnit.MILLIS)) > timestampMsTolerance
      case (Some(l: java.time.Instant), Some(r: java.time.Instant)) =>
        l != r
      case (Some(l: Number), Some(r: Number)) =>
        NumericComparison.compareWithPolicy(
          l,
          r,
          floatingPointTolerance,
          numericTypePolicy
        ) != ComparisonResult.Equal

      // CQL blobs get converted to byte buffers by the Java driver, and the
      // byte buffers are converted to byte arrays by the Spark connector.
      // Arrays can't be compared with standard equality and must be compared
      // with `sameElements`.
      case (Some(l: Array[Byte]), Some(r: Array[Byte])) =>
        !java.util.Arrays.equals(l, r)
      case (Some(l: Array[_]), Some(r: Array[_])) =>
        !l.sameElements(r)

      // Special cases for DynamoDB item values
      case (Some(DdbValue.N(l)), Some(DdbValue.N(r))) =>
        NumericComparison.areNumericalValuesDifferent(
          BigDecimal(l),
          BigDecimal(r),
          floatingPointTolerance
        )
      case (Some(DdbValue.Ns(l)), Some(DdbValue.Ns(r))) =>
        val xs = l.toSeq.map(BigDecimal(_)).sorted
        val ys = r.toSeq.map(BigDecimal(_)).sorted
        xs.size != ys.size || xs.zip(ys).exists { case (x, y) =>
          NumericComparison.areNumericalValuesDifferent(
            x,
            y,
            floatingPointTolerance
          )
        }
      case (Some(DdbValue.L(l)), Some(DdbValue.L(r))) =>
        l.size != r.size || l.zip(r).exists { case (lv, rv) =>
          areDifferent(
            Some(lv),
            Some(rv),
            timestampMsTolerance,
            floatingPointTolerance,
            numericTypePolicy
          )
        }
      case (Some(DdbValue.M(l)), Some(DdbValue.M(r))) =>
        l.size != r.size || l.keySet != r.keySet || l.exists { case (k, lv) =>
          areDifferent(
            Some(lv),
            r.get(k),
            timestampMsTolerance,
            floatingPointTolerance,
            numericTypePolicy
          )
        }

      // All remaining types get compared with standard equality
      case (Some(l), Some(r)) => l != r
      case (Some(_), None)    => true
      case (None, Some(_))    => true
      case (None, None)       => false
    }
}
