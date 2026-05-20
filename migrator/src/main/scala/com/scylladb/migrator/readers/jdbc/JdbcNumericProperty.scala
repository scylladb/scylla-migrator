package com.scylladb.migrator.readers.jdbc

import scala.util.Try

/** Case-insensitive parser for numeric JDBC connection properties.
  *
  * Used by backends (e.g. MySQL) to safely extract optional integer properties from a user-supplied
  * `connectionProperties` map. The parser rejects:
  *   - duplicate entries with different casing (ambiguous and likely user error),
  *   - non-digit values (e.g. `"5000&allowMultiQueries=true"` URL-injection attempts),
  *   - values outside the configured `[min, max]` range.
  *
  * Error strings are stable and asserted on by tests.
  */
object JdbcNumericProperty {

  private val DigitsOnly = """\d+""".r

  /** Declarative spec for a numeric JDBC property. `expectation` is the human-readable phrase
    * embedded into validation messages (e.g. "a positive integer number of bytes").
    */
  final case class Spec(
    name: String,
    minimum: Long,
    maximum: Long,
    defaultValue: Long,
    expectation: String
  )

  private def caseInsensitiveMatches(
    props: Map[String, String],
    propertyName: String
  ): List[(String, String)] =
    props.toList.filter { case (k, _) => k.equalsIgnoreCase(propertyName) }

  /** Parse a single spec out of `props`. Returns `Left(error)` on rejection, `Right(value)` on
    * success, falling back to `spec.defaultValue` when the key is absent.
    *
    * Leading/trailing whitespace in the raw value is trimmed before the digit-pattern check, so a
    * user-supplied `" 1000 "` parses successfully. Trimming does NOT weaken the security regex:
    * `DigitsOnly` still rejects any embedded whitespace or punctuation (e.g.
    * `"1000&allowMultiQueries=true"` after trim still falls to the error branch). The error message
    * preserves the trimmed value so log lines remain stable.
    */
  def parse(props: Map[String, String], spec: Spec): Either[String, Long] =
    caseInsensitiveMatches(props, spec.name) match {
      case Nil => Right(spec.defaultValue)
      case List((_, rawValue)) =>
        val trimmed = rawValue.trim
        trimmed match {
          case DigitsOnly() =>
            val parsed = Try(trimmed.toLong).getOrElse(Long.MinValue)
            if (parsed < spec.minimum || parsed > spec.maximum)
              Left(s"${spec.name} must be ${spec.expectation}, got: '$trimmed'")
            else
              Right(parsed)
          case _ =>
            Left(s"${spec.name} must be ${spec.expectation}, got: '$trimmed'")
        }
      case matches =>
        Left(
          s"connectionProperties contains duplicate entries for '${spec.name}' with different casing: " +
            matches.map(_._1).mkString(", ")
        )
    }

  /** Validate every spec, returning the collected list of error messages (empty if all valid).
    * `connectionProperties = None` is treated as an empty map.
    */
  def validateAll(
    connectionProperties: Option[Map[String, String]],
    specs: Seq[Spec]
  ): List[String] = {
    val userProps = connectionProperties.getOrElse(Map.empty)
    specs.toList.flatMap(spec => parse(userProps, spec).left.toOption)
  }

  /** Parse-or-throw helper. Used by URL builders where validation has already run, so a parse
    * failure indicates a programmatic bypass of the config decoder.
    */
  def parseOrThrow(props: Map[String, String], spec: Spec): Long =
    parse(props, spec).fold(error => throw new IllegalArgumentException(error), identity)
}
