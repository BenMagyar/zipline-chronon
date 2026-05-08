package ai.chronon.spark.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeIntoTable, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.stats.PrepareDeltaScan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/** Workaround for delta-io/delta#5804 on EMR where Delta's PrepareDeltaScan incorrectly
  * skips ALL V2WriteCommand plans (including Iceberg writes reading from Delta sources with DVs).
  *
  * Registered AFTER DeltaSparkSessionExtension in spark.sql.extensions so this rule runs
  * after Delta's PrepareDeltaScan. For V2 writes without V1 fallback (e.g. Iceberg), and
  * for MERGE source plans, we apply PrepareDeltaScan to the source table scans that Delta's
  * rule skipped.
  */
class ChrononDeltaFixExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPostHocResolutionRule(session => new DeltaScanFixRule(session))
    extensions.injectPreCBORule(session => new DeltaScanFixRule(session))
  }
}

private[extensions] class DeltaScanFixRule(session: SparkSession) extends Rule[LogicalPlan] {
  private lazy val delegate = new PrepareDeltaScan(session)

  private def hasV1Fallback(table: Any): Boolean =
    table.getClass.getInterfaces.exists(_.getSimpleName == "V2TableWithV1Fallback")

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case v2: V2WriteCommand =>
        val targetHasV1Fallback = v2.table match {
          case r: DataSourceV2Relation => hasV1Fallback(r.table)
          case _                       => false
        }
        if (targetHasV1Fallback) {
          plan
        } else {
          plan.mapChildren(delegate.apply)
        }
      case merge: MergeIntoTable =>
        merge.copy(sourceTable = delegate.apply(merge.sourceTable))
      case _ => plan
    }
  }
}
