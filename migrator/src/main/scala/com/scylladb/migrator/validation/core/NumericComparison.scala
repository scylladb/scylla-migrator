package com.scylladb.migrator.validation.core

sealed trait NumericTypePolicy
object NumericTypePolicy {
  case object Lenient extends NumericTypePolicy
  case object DetectWiden extends NumericTypePolicy
  case object StrictType extends NumericTypePolicy

  implicit val encoder: io.circe.Encoder[NumericTypePolicy] =
    io.circe.Encoder.encodeString.contramap {
      case Lenient     => "Lenient"
      case DetectWiden => "DetectWiden"
      case StrictType  => "StrictType"
    }

  implicit val decoder: io.circe.Decoder[NumericTypePolicy] =
    io.circe.Decoder.decodeString.emap {
      case s if s.equalsIgnoreCase("Lenient")     => Right(Lenient)
      case s if s.equalsIgnoreCase("DetectWiden") => Right(DetectWiden)
      case s if s.equalsIgnoreCase("StrictType")  => Right(StrictType)
      case other =>
        Left(s"Invalid numericTypePolicy '$other'. Allowed: Lenient, DetectWiden, StrictType")
    }
}

sealed trait ComparisonResult
object ComparisonResult {
  case object Equal extends ComparisonResult
  case object Different extends ComparisonResult
  case class TypeMismatch(srcType: String, tgtType: String) extends ComparisonResult
}

object NumericComparison {
  import ComparisonResult._

  private sealed trait FloatingPointSpecial
  private case object NaNValue extends FloatingPointSpecial
  private case class InfinityValue(sign: Int) extends FloatingPointSpecial

  def compareWithPolicy(
    left: Number,
    right: Number,
    tolerance: Double,
    policy: NumericTypePolicy
  ): ComparisonResult = {
    val areValuesDifferent = areNumbersDifferent(left, right, tolerance)
    if (areValuesDifferent) {
      Different
    } else {
      policy match {
        case NumericTypePolicy.Lenient => Equal
        case NumericTypePolicy.StrictType =>
          if (left.getClass != right.getClass)
            TypeMismatch(left.getClass.getSimpleName, right.getClass.getSimpleName)
          else
            Equal
        case NumericTypePolicy.DetectWiden =>
          val leftIsFloat = left.isInstanceOf[java.lang.Float]
          val leftIsDouble = left.isInstanceOf[java.lang.Double]
          val rightIsFloat = right.isInstanceOf[java.lang.Float]
          val rightIsDouble = right.isInstanceOf[java.lang.Double]

          if ((leftIsFloat && rightIsDouble) || (leftIsDouble && rightIsFloat)) {
            val (fVal, dVal) = if (leftIsFloat) {
              (left.floatValue(), right.doubleValue())
            } else {
              (right.floatValue(), left.doubleValue())
            }
            if ((fVal.isNaN && dVal.isNaN) || fVal.toDouble == dVal) {
              Equal
            } else {
              TypeMismatch(left.getClass.getSimpleName, right.getClass.getSimpleName)
            }
          } else {
            Equal
          }
      }
    }
  }

  def areNumbersDifferent(left: Number, right: Number, tolerance: Double): Boolean = {
    val leftSpecial = floatingPointSpecial(left)
    val rightSpecial = floatingPointSpecial(right)

    (leftSpecial, rightSpecial) match {
      case (Some(NaNValue), Some(NaNValue))                         => false
      case (Some(InfinityValue(lSign)), Some(InfinityValue(rSign))) => lSign != rSign
      case (Some(_), _) | (_, Some(_))                              => true
      case (None, None) =>
        if (tolerance > 0) {
          (normalizedDecimalValue(left), normalizedDecimalValue(right)) match {
            case (Some(l), Some(r)) => areNumericalValuesDifferent(l, r, tolerance)
            case _                  => left != right
          }
        } else {
          (normalizedIntegralValue(left), normalizedIntegralValue(right)) match {
            case (Some(l), Some(r)) => l != r
            case _ =>
              (normalizedDecimalValue(left), normalizedDecimalValue(right)) match {
                case (Some(l), Some(r)) => areNumericalValuesDifferent(l, r, tolerance)
                case _                  => left != right
              }
          }
        }
    }
  }

  private def floatingPointSpecial(value: Number): Option[FloatingPointSpecial] =
    value match {
      case d: java.lang.Double if d.isNaN      => Some(NaNValue)
      case d: java.lang.Double if d.isInfinite => Some(InfinityValue(Math.signum(d).toInt))
      case f: java.lang.Float if f.isNaN       => Some(NaNValue)
      case f: java.lang.Float if f.isInfinite  => Some(InfinityValue(Math.signum(f).toInt))
      case _                                   => None
    }

  def normalizedIntegralValue(value: Number): Option[BigInt] =
    value match {
      case b: java.lang.Byte        => Some(BigInt(b.longValue))
      case s: java.lang.Short       => Some(BigInt(s.longValue))
      case i: java.lang.Integer     => Some(BigInt(i.longValue))
      case l: java.lang.Long        => Some(BigInt(l.longValue))
      case bi: java.math.BigInteger => Some(BigInt(bi))
      case bi: BigInt               => Some(bi)
      case bd: java.math.BigDecimal =>
        val stripped = bd.stripTrailingZeros
        if (stripped.scale <= 0) Some(BigInt(stripped.toBigIntegerExact)) else None
      case bd: BigDecimal =>
        val stripped = bd.bigDecimal.stripTrailingZeros
        if (stripped.scale <= 0) Some(BigInt(stripped.toBigIntegerExact)) else None
      case _ => None
    }

  def normalizedDecimalValue(value: Number): Option[BigDecimal] =
    value match {
      case d: java.lang.Double if d.isNaN || d.isInfinite => None
      case f: java.lang.Float if f.isNaN || f.isInfinite  => None
      case b: java.lang.Byte                              => Some(BigDecimal(BigInt(b.longValue)))
      case s: java.lang.Short                             => Some(BigDecimal(BigInt(s.longValue)))
      case i: java.lang.Integer                           => Some(BigDecimal(BigInt(i.longValue)))
      case l: java.lang.Long                              => Some(BigDecimal(BigInt(l.longValue)))
      case bi: java.math.BigInteger                       => Some(BigDecimal(BigInt(bi)))
      case bi: BigInt                                     => Some(BigDecimal(bi))
      case bd: java.math.BigDecimal                       => Some(BigDecimal(bd))
      case bd: BigDecimal                                 => Some(bd)
      case f: java.lang.Float                             => Some(BigDecimal.decimal(f.doubleValue))
      case d: java.lang.Double                            => Some(BigDecimal.decimal(d.doubleValue))
      case _                                              => None
    }

  def areNumericalValuesDifferent(
    x: BigDecimal,
    y: BigDecimal,
    tolerance: BigDecimal
  ): Boolean =
    (x - y).abs > tolerance
}
