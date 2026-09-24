package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.{MetaData, PartitionGrid, PartitionSpec, TableDependency}
import ai.chronon.planner.ExternalSourceSensorNode

import scala.collection.JavaConverters._

object ExternalSourceSensorUtil {

  // Sensors only run lightweight partition-check queries — override resource-heavy
  // configs inherited from downstream nodes with minimal values. Sensors spend most of
  // their runtime sleeping between polls while serverless platforms bill the held vCPUs,
  // so cores stay near the floor: the driver does catalog/manifest metadata reads (2 cores
  // so JVM housekeeping - GC, heartbeats - never starves the query thread), and the single
  // 2-core executor only serves the rare full-scan fallback tier.
  private val SensorResourceOverrides: Map[String, String] = Map(
    "spark.driver.memory" -> "1g",
    "spark.driver.cores" -> "2",
    "spark.executor.memory" -> "1g",
    "spark.executor.cores" -> "2",
    "spark.executor.instances" -> "1",
    "spark.default.parallelism" -> "2",
    "spark.sql.shuffle.partitions" -> "2"
  )

  // Sensors share compute across configurations and cron schedules when they sense the same table
  // with the same semantics. A node has one grid for its step labels, so sensors on different grids
  // need distinct logical names; otherwise the branch's single current hash for that name makes one
  // scheduled configuration use the other configuration's grid. Plain daily keeps the legacy suffix
  // so existing sensors are not renamed on upgrade.
  private[planner] def sensorName(table: String, grid: PartitionGrid): String = {
    def compact(millis: Long): String = WindowUtils.fromMillis(millis).str
    if (grid.isDaily) s"${table}__sensor"
    else if (grid.offsetMillis == 0) s"${table}__${compact(grid.spanMillis)}__sensor"
    else s"${table}__${compact(grid.spanMillis)}-${compact(grid.offsetMillis)}__sensor"
  }

  /** The consumer dependency maps a consumer range onto the source table's range. The orchestrator
    * applies that mapping before it creates a sensor step, so the sensor receives a range that is
    * already expressed on the source table. Its executable dependency must therefore be an identity
    * mapping; retaining offsets or cutoffs would apply the consumer mapping a second time and would
    * also prevent two consumers from sharing sensor work over overlapping source ranges.
    */
  private[planner] def resolvedSensorDependency(tableDependency: TableDependency): TableDependency = {
    val resolved = tableDependency.deepCopy()
    resolved.setStartOffset(WindowUtils.zero())
    resolved.setEndOffset(WindowUtils.zero())
    resolved.unsetStartCutOff()
    resolved.unsetEndCutOff()
    resolved
  }

  def sensorNodes(metaData: MetaData)(implicit spec: PartitionSpec): Seq[ExternalSourceSensorNode] = {

    metaData.executionInfo.tableDependencies.asScala
      .map((td) => {
        val tdSpec = td.tableInfo.partitionSpec(spec)
        val sensorMd = MetaDataUtils.layer(
          metaData,
          "sensor",
          sensorName(td.tableInfo.table, tdSpec.grid),
          Seq(), // No table dependencies for sensors
          outputTableOverride =
            Option(td.tableInfo.table) // The input table and the output table are the same for sensors.
        )(tdSpec)

        applySensorResourceOverrides(sensorMd)

        // The retry budget is one partition interval: the schedule fires a fresh sensor every
        // interval, so a sensor that outlives its own interval is redundant with its successor
        // and only stacks up holding cluster slots. Daily grids keep the historical
        // 96 x 15min = 24h budget exactly; an hourly sensor exits after ~1h. Poll interval
        // scales down for fine grids so every sensor still gets at least a few checks.
        val spanMinutes = math.max(1L, tdSpec.spanMillis / WindowUtils.MinuteMillis)
        val retryIntervalMin = math.min(15L, math.max(1L, spanMinutes / 4))
        // ceiling: coverage must reach at least one full interval even when the poll
        // interval doesn't divide the span (e.g. a 9m grid polling every 2m)
        val retryCount = (spanMinutes + retryIntervalMin - 1) / retryIntervalMin
        new ExternalSourceSensorNode()
          .setSourceTableDependency(resolvedSensorDependency(td))
          .setMetaData(sensorMd)
          .setRetryCount(retryCount)
          .setRetryIntervalMin(retryIntervalMin)
      })
      .toList
  }

  private def applySensorResourceOverrides(metaData: MetaData): Unit = {
    val execInfo = Option(metaData.executionInfo).getOrElse(return
    )
    val conf = Option(execInfo.conf).getOrElse(return
    )
    val common = Option(conf.common).getOrElse(return
    )
    SensorResourceOverrides.foreach { case (k, v) =>
      common.put(k, v)
    }
  }

}
