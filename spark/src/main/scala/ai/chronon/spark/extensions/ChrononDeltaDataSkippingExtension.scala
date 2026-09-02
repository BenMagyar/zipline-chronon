package ai.chronon.spark.extensions

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Or}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.stats.{DataSize, DeltaScan, DeltaScanGenerator, PrepareDeltaScan}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/** Workaround for delta-io/delta#6756. Applies Delta data-skipping predicates independently before
  * combining their candidate files.
  *
  * Delta normally disables an entire compound predicate for a file when any referenced statistic
  * is missing. Evaluating independent referenced-column groups and nested AND branches lets a
  * complete side of an AND prune the file while retaining Delta's conservative behavior for
  * missing statistics.
  *
  * Register this extension before `io.delta.sql.DeltaSparkSessionExtension`. Delta's own
  * `PrepareDeltaScan` will see the prepared file index produced here and leave it unchanged.
  * Remove this extension once the Delta version used by Chronon includes the upstream fix.
  */
class ChrononDeltaDataSkippingExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPreCBORule(session => new PartialStatsPrepareDeltaScan(session))
  }
}

private[extensions] class PartialStatsPrepareDeltaScan(spark: SparkSession) extends PrepareDeltaScan(spark) {

  private case class CandidateFiles(paths: Set[String], scanDurationMs: Long) {
    def and(other: CandidateFiles): CandidateFiles =
      CandidateFiles(paths intersect other.paths, scanDurationMs + other.scanDurationMs)

    def or(other: CandidateFiles): CandidateFiles =
      CandidateFiles(paths union other.paths, scanDurationMs + other.scanDurationMs)
  }

  override protected def filesForScan(scanGenerator: DeltaScanGenerator,
                                      limitOpt: Option[Int],
                                      filters: Seq[Expression],
                                      delta: LogicalRelation): DeltaScan = {
    val baseScan = super.filesForScan(scanGenerator, limitOpt, filters, delta)
    val hasSplittableAnd = filters.map(_.references).distinct.size > 1 || filters.exists(containsSplittableAnd)

    if (limitOpt.nonEmpty || baseScan.files.isEmpty || !hasSplittableAnd) {
      baseScan
    } else {
      // Spark produces separate is-not-null and comparison filters for a single SQL predicate.
      // Keep those in one Delta scan to avoid duplicate metadata work for the same columns.
      val candidates = filters
        .groupBy(_.references)
        .values
        .map { sameColumns =>
          candidateFiles(scanGenerator, delta, sameColumns.reduce(And))
        }
        .reduce(_.and(_))
      val refinedFiles = baseScan.files.filter(file => candidates.paths.contains(file.path))

      if (refinedFiles.size == baseScan.files.size) {
        baseScan
      } else {
        DeltaScan(
          version = baseScan.version,
          files = refinedFiles,
          total = baseScan.total,
          partition = baseScan.partition,
          scanned = dataSize(refinedFiles)
        )(
          scannedSnapshot = baseScan.scannedSnapshot,
          partitionFilters = baseScan.partitionFilters,
          dataFilters = baseScan.dataFilters,
          partitionLikeDataFilters = baseScan.partitionLikeDataFilters,
          rewrittenPartitionLikeDataFilters = baseScan.rewrittenPartitionLikeDataFilters,
          unusedFilters = baseScan.unusedFilters,
          scanDurationMs = baseScan.scanDurationMs + candidates.scanDurationMs,
          dataSkippingType = baseScan.dataSkippingType
        )
      }
    }
  }

  private def containsSplittableAnd(expression: Expression): Boolean =
    expression match {
      case And(left, right) =>
        left.references != right.references ||
        containsSplittableAnd(left) ||
        containsSplittableAnd(right)
      case Or(left, right) =>
        containsSplittableAnd(left) || containsSplittableAnd(right)
      case _ => false
    }

  private def candidateFiles(scanGenerator: DeltaScanGenerator,
                             delta: LogicalRelation,
                             expression: Expression): CandidateFiles =
    expression match {
      case And(left, right) if containsSplittableAnd(expression) =>
        candidateFiles(scanGenerator, delta, left).and(candidateFiles(scanGenerator, delta, right))
      case Or(left, right) if containsSplittableAnd(expression) =>
        candidateFiles(scanGenerator, delta, left).or(candidateFiles(scanGenerator, delta, right))
      case leaf =>
        candidateFilesForLeaf(scanGenerator, delta, leaf)
    }

  private def candidateFilesForLeaf(scanGenerator: DeltaScanGenerator,
                                    delta: LogicalRelation,
                                    expression: Expression): CandidateFiles = {
    val scan = super.filesForScan(scanGenerator, limitOpt = None, filters = Seq(expression), delta)
    CandidateFiles(scan.files.iterator.map(_.path).toSet, scan.scanDurationMs)
  }

  private def dataSize(files: Seq[AddFile]): DataSize =
    DataSize(
      bytesCompressed = Some(files.iterator.map(_.size).sum),
      rows = sumIfComplete(files.map(_.numPhysicalRecords)),
      files = Some(files.size.toLong),
      logicalRows = sumIfComplete(files.map(_.numLogicalRecords))
    )

  private def sumIfComplete(values: Seq[Option[Long]]): Option[Long] =
    if (values.forall(_.isDefined)) Some(values.iterator.flatten.sum) else None
}
