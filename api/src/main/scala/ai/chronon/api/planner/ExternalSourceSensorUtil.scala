package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.{MetaData, PartitionGrid, PartitionSpec}
import ai.chronon.planner.ExternalSourceSensorNode

import scala.collection.JavaConverters._

object ExternalSourceSensorUtil {

  // Sensors only run lightweight partition-check queries — override resource-heavy
  // configs inherited from downstream nodes with minimal values.
  private val SensorResourceOverrides: Map[String, String] = Map(
    "spark.driver.memory" -> "1g",
    "spark.driver.cores" -> "4",
    "spark.executor.memory" -> "1g",
    "spark.executor.cores" -> "4",
    "spark.executor.instances" -> "1",
    "spark.default.parallelism" -> "4",
    "spark.sql.shuffle.partitions" -> "4"
  )

  def semanticExternalSourceSensor(sensorNode: ExternalSourceSensorNode): ExternalSourceSensorNode = {
    val semanticSensor = sensorNode.deepCopy()
    semanticSensor.unsetMetaData()
    semanticSensor
  }

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

        val retryInterval = 15 // minutes
        val retryCount = 96
        new ExternalSourceSensorNode()
          .setSourceTableDependency(td)
          .setMetaData(sensorMd)
          .setRetryCount(retryCount)
          .setRetryIntervalMin(retryInterval)
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
