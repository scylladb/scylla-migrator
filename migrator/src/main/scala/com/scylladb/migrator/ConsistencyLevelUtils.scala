package com.scylladb.migrator

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.datastax.oss.driver.api.core.ConsistencyLevel

object ConsistencyLevelUtils {

  /** Parse a consistency level string into a [[ConsistencyLevel]]. Throws
    * [[IllegalArgumentException]] if the string is not a valid consistency level.
    */
  def parseConsistencyLevel(configured: String): ConsistencyLevel =
    try DefaultConsistencyLevel.valueOf(configured)
    catch {
      case _: IllegalArgumentException =>
        val validValues = DefaultConsistencyLevel.values().map(_.name()).mkString(", ")
        throw new IllegalArgumentException(
          s"Invalid consistency level '${configured}'. Valid values are: ${validValues}"
        )
    }
}
