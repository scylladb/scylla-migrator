package com.scylladb.migrator.config

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

import java.util.Locale

case class Rename(from: String, to: String)
object Rename {
  implicit val encoder: Encoder[Rename] = deriveEncoder[Rename]
  implicit val decoder: Decoder[Rename] = deriveDecoder[Rename]

  /** Build a rename map keyed by lowercase source column name. Detects conflicting case-insensitive
    * mappings (e.g. "Foo"→"bar" and "foo"→"baz").
    */
  def buildCaseInsensitiveMap(renames: List[Rename]): Map[String, String] =
    renames
      .groupBy(_.from.toLowerCase(Locale.ROOT))
      .view
      .map { case (lowerKey, entries) =>
        val targets = entries.map(_.to).distinct
        if (targets.size > 1)
          sys.error(
            s"Renames contain conflicting case-insensitive mappings for source column '$lowerKey': " +
              s"${targets.mkString(", ")}"
          )
        lowerKey -> targets.head
      }
      .toMap

  /** Resolve a column name through a case-insensitive rename map, falling back to identity. */
  def resolveRename(caseInsensitiveMap: Map[String, String], column: String): String =
    caseInsensitiveMap.getOrElse(column.toLowerCase(Locale.ROOT), column)

  /** Detect case-insensitive collisions in the *target* side of renames. Returns error messages for
    * each collision group, or empty list if clean.
    */
  def detectTargetCollisions(renames: List[Rename]): List[String] =
    renames
      .groupBy(_.to.toLowerCase(Locale.ROOT))
      .collect {
        case (_, entries) if entries.map(_.from).distinct.size > 1 =>
          val sources = entries.map(_.from).distinct
          s"Multiple source columns (${sources.mkString(", ")}) rename to the same target '${entries.head.to}'"
      }
      .toList
}
