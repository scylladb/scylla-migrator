package com.scylladb.migrator

import com.scylladb.migrator.config.MigratorConfig
import org.apache.hadoop.conf.Configuration
import org.apache.logging.log4j.LogManager

import scala.util.control.NonFatal

/** Startup auto-resume.
  *
  * Merges the skip-state of the most recent savepoint into the running configuration so that
  * re-running the same configuration continues where the previous run left off, without the
  * operator having to repoint the configuration at the latest savepoint file by hand.
  *
  * Only the skip-state fields (`skipTokenRanges`/`skipSegments`/`skipParquetFiles`) are taken from
  * the savepoint. Every other setting - crucially the source/target credentials - comes from the
  * live configuration, so this works even though savepoint files are written with secrets redacted
  * (see `SavepointsManager.doDump`).
  *
  * Any failure to read or parse prior savepoints is non-fatal: the migration simply proceeds with
  * the provided configuration rather than aborting.
  */
object SavepointsResume {
  private val log = LogManager.getLogger(getClass)

  def resume(
    config: MigratorConfig,
    baseHadoopConfiguration: Option[Configuration],
    redactionRegex: Option[String]
  ): MigratorConfig =
    if (!config.source.supportsSavepoints || !config.savepoints.autoResume) config
    else
      try {
        val hadoopConfiguration =
          SavepointStorage.hadoopConfiguration(config, baseHadoopConfiguration, redactionRegex)
        val storagePath = config.savepoints.storagePath
        val pathIO = PathIO.forPath(storagePath, Some(hadoopConfiguration))

        if (!pathIO.exists(storagePath)) {
          log.info(
            s"Auto-resume: savepoints location ${pathIO.normalize(storagePath)} does not exist " +
              s"yet; starting from the provided configuration."
          )
          config
        } else {
          // Walk savepoints newest-first and resume from the first one that parses. A single
          // corrupt or partially-written latest savepoint (e.g. an in-flight object-store write)
          // must not discard older, still-recoverable progress, so per-file read/parse errors are
          // logged and skipped rather than aborting the whole resume.
          val firstValidSavepoint =
            SavepointsManager
              .savepointNamesNewestFirst(pathIO.listFileNames(storagePath))
              .iterator
              .flatMap { name =>
                val candidatePath = s"${storagePath}/${name}"
                try
                  Some(candidatePath -> MigratorConfig.loadFrom(candidatePath, hadoopConfiguration))
                catch {
                  case NonFatal(e) =>
                    log.warn(
                      s"Auto-resume: skipping unreadable savepoint " +
                        s"${pathIO.normalize(candidatePath)}; trying an older one.",
                      e
                    )
                    None
                }
              }
              .nextOption()

          firstValidSavepoint match {
            case None =>
              log.info(
                s"Auto-resume: no usable savepoint found in ${pathIO.normalize(storagePath)}; " +
                  s"starting from the provided configuration."
              )
              config
            case Some((latestPath, savepoint)) =>
              val merged = mergeSkipState(config, savepoint)
              log.info(
                s"Auto-resume: merged skip-state from ${pathIO.normalize(latestPath)} " +
                  s"(skipTokenRanges=${merged.skipTokenRanges.fold(0)(_.size)}, " +
                  s"skipSegments=${merged.skipSegments.fold(0)(_.size)}, " +
                  s"skipParquetFiles=${merged.skipParquetFiles.fold(0)(_.size)})."
              )
              merged
          }
        }
      } catch {
        case NonFatal(e) =>
          log.warn(
            s"Auto-resume: could not load prior savepoint state from " +
              s"${config.savepoints.storagePath}; starting from the provided configuration.",
            e
          )
          config
      }

  /** Union the skip-state of `savepoint` into `config`. A field stays `None` only when both sides
    * are `None`, so a freshly merged config never spuriously gains an empty skip set.
    */
  private[migrator] def mergeSkipState(
    config: MigratorConfig,
    savepoint: MigratorConfig
  ): MigratorConfig =
    config.copy(
      skipTokenRanges  = unionOption(config.skipTokenRanges, savepoint.skipTokenRanges),
      skipSegments     = unionOption(config.skipSegments, savepoint.skipSegments),
      skipParquetFiles = unionOption(config.skipParquetFiles, savepoint.skipParquetFiles)
    )

  private def unionOption[A](left: Option[Set[A]], right: Option[Set[A]]): Option[Set[A]] =
    (left, right) match {
      case (None, None) => None
      case _            => Some(left.getOrElse(Set.empty) ++ right.getOrElse(Set.empty))
    }
}
