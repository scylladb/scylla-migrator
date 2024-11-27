package com.scylladb.migrator.validation

import com.datastax.spark.connector.CassandraRow
import com.google.common.math.DoubleMath

import java.time.temporal.ChronoUnit

case class RowComparisonFailure(rowRepr: String,
                                otherRepr: Option[String],
                                items: List[RowComparisonFailure.Item]) {

  override def toString: String =
    s"""
       |Row failure:
       |* Source row: ${rowRepr}
       |* Target row: ${otherRepr.getOrElse("<MISSING>")}
       |* Failures:
       |${items.map(item => s"  - ${item.description}").mkString("\n")}
     """.stripMargin
}

object RowComparisonFailure {
  sealed abstract class Item(val description: String) extends Serializable
  object Item {
    case object MissingTargetRow extends Item("Missing target row")
    case object MismatchedColumnCount extends Item("Mismatched column count")
    case object MismatchedColumnNames extends Item("Mismatched column names")
    case class DifferingFieldValues(fields: List[String])
        extends Item(s"Differing fields: ${fields.mkString(", ")}")
    case class DifferingTtls(details: List[(String, Long)])
        extends Item(s"Differing TTLs: ${details
          .map {
            case (fieldName, ttlDiff) => s"$fieldName ($ttlDiff millis)"
          }
          .mkString(", ")}")
    case class DifferingWritetimes(details: List[(String, Long)])
        extends Item(s"Differing WRITETIMEs: ${details
          .map {
            case (fieldName, writeTimeDiff) => s"$fieldName ($writeTimeDiff millis)"
          }
          .mkString(", ")}")
  }

  def cassandraRowComparisonFailure(left: CassandraRow,
                                    right: Option[CassandraRow],
                                    items: List[Item]): RowComparisonFailure =
    RowComparisonFailure(left.toString, right.map(_.toString), items)

  def compareCassandraRows(left: CassandraRow,
                           right: Option[CassandraRow],
                           floatingPointTolerance: Double,
                           timestampMsTolerance: Long,
                           ttlToleranceMillis: Long,
                           writetimeToleranceMillis: Long,
                           compareTimestamps: Boolean): Option[RowComparisonFailure] =
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

        val differingFieldValues =
          for {
            name <- names
            if !name.endsWith("_ttl") && !name.endsWith("_writetime")
            leftValue  = leftMap.get(name)
            rightValue = rightMap.get(name)
            if areDifferent(leftValue, rightValue, timestampMsTolerance, floatingPointTolerance)
          } yield name

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

        if (differingFieldValues.isEmpty && differingTtls.isEmpty && differingWritetimes.isEmpty)
          None
        else
          Some(
            cassandraRowComparisonFailure(
              left,
              Some(right),
              (if (differingFieldValues.nonEmpty)
                 List(Item.DifferingFieldValues(differingFieldValues.toList))
               else Nil) ++
                (if (differingTtls.nonEmpty) List(Item.DifferingTtls(differingTtls.toList))
                 else Nil) ++
                (if (differingWritetimes.nonEmpty)
                   List(Item.DifferingWritetimes(differingWritetimes.toList))
                 else Nil)
            )
          )
    }

  /**
    * @param leftValue              First value to compare
    * @param rightValue             Second value to compare
    * @param timestampMsTolerance   Tolerance when comparing instant values
    * @param floatingPointTolerance Tolerance when comparing floating point values
    * @return `true` if the `leftValue` is different from the `rightValue`
    */
  private def areDifferent(leftValue: Option[Any],
                           rightValue: Option[Any],
                           timestampMsTolerance: Long,
                           floatingPointTolerance: Double): Boolean =
    (rightValue, leftValue) match {
      // All timestamp types need to be compared with a configured tolerance
      case (Some(l: java.time.Instant), Some(r: java.time.Instant)) if timestampMsTolerance > 0 =>
        Math.abs(r.until(l, ChronoUnit.MILLIS)) > timestampMsTolerance
      // All floating-point-like types need to be compared with a configured tolerance
      case (Some(l: Float), Some(r: Float)) =>
        !DoubleMath.fuzzyEquals(l, r, floatingPointTolerance)
      case (Some(l: Double), Some(r: Double)) =>
        !DoubleMath.fuzzyEquals(l, r, floatingPointTolerance)
      case (Some(l: java.math.BigDecimal), Some(r: java.math.BigDecimal)) =>
        areNumericalValuesDifferent(l, r, floatingPointTolerance)

      // CQL blobs get converted to byte buffers by the Java driver, and the
      // byte buffers are converted to byte arrays by the Spark connector.
      // Arrays can't be compared with standard equality and must be compared
      // with `sameElements`.
      case (Some(l: Array[_]), Some(r: Array[_])) =>
        !l.sameElements(r)

      // All remaining types get compared with standard equality
      case (Some(l), Some(r)) => l != r
      case (Some(_), None)    => true
      case (None, Some(_))    => true
      case (None, None)       => false
    }

  /** @return true iff the difference between `x` and `y` is greater than the `tolerance` */
  private def areNumericalValuesDifferent(x: BigDecimal,
                                          y: BigDecimal,
                                          tolerance: BigDecimal): Boolean =
    (x - y).abs > tolerance

}
