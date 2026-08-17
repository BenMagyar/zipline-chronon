package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.{MetaData, PartitionGrid, PartitionSpec}
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

  // Sensor node names must be unique per (table, dependency grid): two confs watching the same
  // table on different grids (e.g. a 3h@1h staging query and a 1d@1h groupBy) would otherwise
  // emit sensors with the same name, and the hub keys nodes by name per branch — one grid
  // clobbers the other and the losing conf fails its partition-grid check every run. Plain daily
  // keeps the legacy suffix so existing sensors aren't renamed on upgrade.
  private[planner] def sensorName(table: String, grid: PartitionGrid): String = {
    def compact(millis: Long): String = WindowUtils.fromMillis(millis).str
    if (grid.isDaily) s"${table}__sensor"
    else if (grid.offsetMillis == 0) s"${table}__${compact(grid.spanMillis)}__sensor"
    else s"${table}__${compact(grid.spanMillis)}-${compact(grid.offsetMillis)}__sensor"
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
          .setSourceTableDependency(td)
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
