package com.scylladb.migrator.config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.regex.Pattern
import scala.util.Try

object SparkSecretRedaction {
  val RedactionRegexConfKey: String = "spark.redaction.regex"
  private[config] val SparkDefaultRedactionRegex: String =
    "(?i)secret|password|token|access[.]?key"

  private def configuredRedactionRegex(regex: Option[String]): Option[String] =
    regex.map(_.trim).filter(_.nonEmpty)

  def ensureMigratorRedactionRegex(sparkConf: SparkConf): Unit =
    if (configuredRedactionRegex(sparkConf.getOption(RedactionRegexConfKey)).isEmpty)
      sparkConf.set(RedactionRegexConfKey, SensitiveKeys.DefaultRedactionRegex)

  private def installMigratorRedactionRegex(sparkConf: SparkConf): String = {
    sparkConf.set(RedactionRegexConfKey, SensitiveKeys.DefaultRedactionRegex)
    SensitiveKeys.DefaultRedactionRegex
  }

  private def sparkContextRedactionRegex(spark: SparkSession): Option[String] =
    configuredRedactionRegex(spark.sparkContext.getConf.getOption(RedactionRegexConfKey))

  private def sparkSessionRedactionRegex(spark: SparkSession): Option[String] =
    configuredRedactionRegex(spark.conf.getAll.get(RedactionRegexConfKey))

  def redactionRegex(spark: SparkSession): Option[String] = {
    val sparkConfRegex = sparkContextRedactionRegex(spark)
    val sessionRegex = sparkSessionRedactionRegex(spark)

    sparkConfRegex
      .orElse {
        sessionRegex.filter(_ == SparkDefaultRedactionRegex)
      }
      .orElse {
        sessionRegex.foreach { regex =>
          throw new IllegalStateException(
            s"$RedactionRegexConfKey must be configured on SparkConf before SparkSession creation; " +
              s"runtime SparkSession value '$regex' is not verified for Spark and Hadoop redaction"
          )
        }
        Some(SparkDefaultRedactionRegex)
      }
  }

  def redactionRegex(sparkConf: SparkConf): Option[String] =
    Some(
      configuredRedactionRegex(sparkConf.getOption(RedactionRegexConfKey))
        .getOrElse(installMigratorRedactionRegex(sparkConf))
    )

  def sensitiveKeys(keys: Iterable[String]): Seq[String] =
    keys.toSeq.distinct.filter(SensitiveKeys.isSensitiveKey)

  def redactionRegexCoversKeys(
    regex: String,
    keys: Seq[String]
  ): Boolean =
    Try(Pattern.compile(regex)).toOption.exists { pattern =>
      keys.forall(key => pattern.matcher(key).find())
    }

  def ensureKeysRedacted(
    redactionRegex: Option[String],
    optionKeys: Iterable[String],
    context: String
  ): Unit = {
    val keys = sensitiveKeys(optionKeys)
    if (keys.nonEmpty) {
      val regex = redactionRegex
        .getOrElse(SensitiveKeys.DefaultRedactionRegex)

      require(
        redactionRegexCoversKeys(regex, keys),
        s"Refusing to create $context because $RedactionRegexConfKey does not redact all sensitive option keys: ${keys.mkString(", ")}"
      )
    }
  }

  def ensureKeysRedacted(
    spark: SparkSession,
    optionKeys: Iterable[String],
    context: String
  ): Unit =
    ensureKeysRedacted(redactionRegex(spark), optionKeys, context)

  def ensureKeysRedacted(
    sparkConf: SparkConf,
    optionKeys: Iterable[String],
    context: String
  ): Unit =
    ensureKeysRedacted(redactionRegex(sparkConf), optionKeys, context)
}
