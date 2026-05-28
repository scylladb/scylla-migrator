package com.scylladb.migrator.config

import java.util.Locale

object SensitiveKeys {
  val DefaultRedactionRegex: String =
    "(?i)password|secret|token|credential|access[._-]?key|api[._-]?key|private[._-]?key|key[._-]?file"

  private val SensitiveKeyMarkers =
    Seq("password", "secret", "token", "credential", "accesskey", "apikey", "privatekey", "keyfile")

  private def normalize(key: String): String =
    key
      .toLowerCase(Locale.ROOT)
      .replace(".", "")
      .replace("_", "")
      .replace("-", "")

  def isSensitiveKey(key: String): Boolean = {
    val normalized = normalize(key)
    SensitiveKeyMarkers.exists(normalized.contains)
  }
}
