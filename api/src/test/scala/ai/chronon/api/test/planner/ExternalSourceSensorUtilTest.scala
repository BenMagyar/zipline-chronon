package ai.chronon.api.test.planner

import ai.chronon.api.planner.ExternalSourceSensorUtil
import ai.chronon.api._
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.planner.ExternalSourceSensorNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class ExternalSourceSensorUtilTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  "semanticExternalSourceSensor" should "create a sensor node without metadata" in {
    val originalMetaData = new MetaData()
      .setName("test_sensor")
      .setTeam("test_team")
      .setVersion("1")

    val td = new TableDependency()
      .setTableInfo(new TableInfo().setTable("test_table"))

    val sensorNode = new ExternalSourceSensorNode()
      .setSourceTableDependency(td)
      .setMetaData(originalMetaData)

    val semanticSensor = ExternalSourceSensorUtil.semanticExternalSourceSensor(sensorNode)

    semanticSensor.sourceTableDependency.tableInfo.table should equal("test_table")
    semanticSensor.metaData should be(null)
    semanticSensor should not be theSameInstanceAs(sensorNode)
  }

  "sensorNodes" should "create sensor nodes for each table dependency" in {
    val tableInfo1 = new TableInfo()
      .setTable("data.purchases")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd")

    val tableInfo2 = new TableInfo()
      .setTable("data.checkouts")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd")

    val tableDep1 = new TableDependency().setTableInfo(tableInfo1)
    val tableDep2 = new TableDependency().setTableInfo(tableInfo2)

    val executionInfo = new ExecutionInfo()
      .setTableDependencies(List(tableDep1, tableDep2).asJava)

    val metaData = new MetaData()
      .setName("test_groupby")
      .setTeam("test_team")
      .setVersion("1")
      .setExecutionInfo(executionInfo)

    val sensorNodes = ExternalSourceSensorUtil.sensorNodes(metaData)

    sensorNodes should have size 2

    val purchasesSensor = sensorNodes.find(_.sourceTableDependency.tableInfo.table == "data.purchases")
    val checkoutsSensor = sensorNodes.find(_.sourceTableDependency.tableInfo.table == "data.checkouts")

    purchasesSensor should be(defined)
    checkoutsSensor should be(defined)

    // Verify metadata structure for purchases sensor
    val purchasesMeta = purchasesSensor.get.metaData
    purchasesMeta.name should endWith("__sensor")
    purchasesMeta.executionInfo.tableDependencies should be(empty)
    purchasesMeta.executionInfo.outputTableInfo.table should equal("data.purchases")

    // Verify metadata structure for checkouts sensor
    val checkoutsMeta = checkoutsSensor.get.metaData
    checkoutsMeta.name should endWith("__sensor")
    checkoutsMeta.executionInfo.tableDependencies should be(empty)
    checkoutsMeta.executionInfo.outputTableInfo.table should equal("data.checkouts")
  }

  "sensorNodes" should "handle empty table dependencies" in {
    val executionInfo = new ExecutionInfo()
      .setTableDependencies(List.empty[TableDependency].asJava)

    val metaData = new MetaData()
      .setName("test_groupby")
      .setTeam("test_team")
      .setVersion("1")
      .setExecutionInfo(executionInfo)

    val sensorNodes = ExternalSourceSensorUtil.sensorNodes(metaData)

    sensorNodes should be(empty)
  }

  "sensorNodes" should "handle null table dependencies" in {
    val executionInfo = new ExecutionInfo()
      .setTableDependencies(null)

    val metaData = new MetaData()
      .setName("test_groupby")
      .setTeam("test_team")
      .setVersion("1")
      .setExecutionInfo(executionInfo)

    an[NullPointerException] should be thrownBy {
      ExternalSourceSensorUtil.sensorNodes(metaData)
    }
  }

  "sensorNodes" should "preserve partition spec in sensor metadata" in {
    val tableInfo = new TableInfo()
      .setTable("data.events")
      .setPartitionColumn("dt")
      .setPartitionFormat("yyyyMMdd")

    val tableDep = new TableDependency().setTableInfo(tableInfo)

    val executionInfo = new ExecutionInfo()
      .setTableDependencies(List(tableDep).asJava)

    val metaData = new MetaData()
      .setName("test_entity")
      .setTeam("test_team")
      .setVersion("1")
      .setExecutionInfo(executionInfo)

    val customPartitionSpec: PartitionSpec = PartitionSpec("dt", "yyyyMMdd", 24 * 60 * 60 * 1000)
    val sensorNodes = ExternalSourceSensorUtil.sensorNodes(metaData)(customPartitionSpec)

    sensorNodes should have size 1
    val sensor = sensorNodes.head
    sensor.metaData.executionInfo.outputTableInfo.partitionColumn should equal("dt")
    sensor.metaData.executionInfo.outputTableInfo.partitionFormat should equal("yyyyMMdd")
  }

  it should "overwrite stale downstream partition fields when sensor output table is overridden" in {
    val dependencySpec = PartitionSpec("dt", "yyyyMMdd", 24 * 60 * 60 * 1000)
    val downstreamSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 60 * 60 * 1000)
    val tableInfo = new TableInfo()
      .setTable("data.events")
      .setPartitionColumn(dependencySpec.column)
      .setPartitionFormat(dependencySpec.format)
      .setPartitionInterval(WindowUtils.fromMillis(dependencySpec.spanMillis))

    val tableDep = new TableDependency().setTableInfo(tableInfo)

    val downstreamOutput = new TableInfo()
      .setTable("data.downstream")
      .setPartitionColumn(downstreamSpec.column)
      .setPartitionFormat(downstreamSpec.format)
      .setPartitionInterval(WindowUtils.fromMillis(downstreamSpec.spanMillis))

    val executionInfo = new ExecutionInfo()
      .setOutputTableInfo(downstreamOutput)
      .setTableDependencies(List(tableDep).asJava)

    val metaData = new MetaData()
      .setName("test_entity")
      .setTeam("test_team")
      .setVersion("1")
      .setExecutionInfo(executionInfo)

    val sensor = ExternalSourceSensorUtil.sensorNodes(metaData)(PartitionSpec.daily).head
    val sensorTableInfo = sensor.metaData.executionInfo.outputTableInfo

    sensorTableInfo.table should equal("data.events")
    sensorTableInfo.partitionColumn should equal(dependencySpec.column)
    sensorTableInfo.partitionFormat should equal(dependencySpec.format)
    sensorTableInfo.partitionInterval should equal(WindowUtils.fromMillis(dependencySpec.spanMillis))
  }

  "sensorNodes" should "create unique sensor names for complex table names" in {
    val tableInfo1 = new TableInfo()
      .setTable("namespace.complex_table_name")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd")

    val tableInfo2 = new TableInfo()
      .setTable("other_namespace.another_table")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd")

    val tableDep1 = new TableDependency().setTableInfo(tableInfo1)
    val tableDep2 = new TableDependency().setTableInfo(tableInfo2)

    val executionInfo = new ExecutionInfo()
      .setTableDependencies(List(tableDep1, tableDep2).asJava)

    val metaData = new MetaData()
      .setName("complex.entity.name.v1")
      .setTeam("complex_team")
      .setVersion("2")
      .setExecutionInfo(executionInfo)

    val sensorNodes = ExternalSourceSensorUtil.sensorNodes(metaData)

    sensorNodes should have size 2

    val sensor1 = sensorNodes.find(_.sourceTableDependency.tableInfo.table == "namespace.complex_table_name")
    val sensor2 = sensorNodes.find(_.sourceTableDependency.tableInfo.table == "other_namespace.another_table")

    sensor1 should be(defined)
    sensor2 should be(defined)

    sensor1.get.metaData.name should endWith("__sensor")
    sensor2.get.metaData.name should endWith("__sensor")

    // Verify sensor names are unique
    val sensorNames = sensorNodes.map(_.metaData.name).toSet
    sensorNames should have size 2
  }

  "sensorNodes" should "give sensors distinct names when confs depend on the same table with different grids" in {
    def metaDataFor(name: String, intervalMillis: Long, offsetMillis: Long): MetaData = {
      val tableInfo = new TableInfo()
        .setTable("data.table_a")
        .setPartitionColumn("ds")
        .setPartitionFormat("yyyy-MM-dd-HH-mm")
        .setPartitionInterval(WindowUtils.fromMillis(intervalMillis))
      if (offsetMillis != 0L) tableInfo.setPartitionOffset(WindowUtils.fromMillis(offsetMillis))
      val executionInfo = new ExecutionInfo()
        .setTableDependencies(List(new TableDependency().setTableInfo(tableInfo)).asJava)
      new MetaData().setName(name).setTeam("test_team").setVersion("1").setExecutionInfo(executionInfo)
    }

    // Two confs watching the same table on different grids (a 3h@1h staging query and a 1d@1h
    // groupBy) used to emit sensors with the SAME node name. branch_nodes keys nodes by name, so
    // whichever conf synced last clobbered the other's sensor grid and the loser failed its
    // partition-grid check every run.
    val hourMillis = 60 * 60 * 1000L
    val threeHourly =
      ExternalSourceSensorUtil.sensorNodes(metaDataFor("test_team.sq__3", 3 * hourMillis, hourMillis)).head
    val dailyOffset =
      ExternalSourceSensorUtil.sensorNodes(metaDataFor("test_team.gb__1", 24 * hourMillis, hourMillis)).head

    threeHourly.metaData.name should not equal dailyOffset.metaData.name
    threeHourly.metaData.name shouldBe "data.table_a__3h-1h__sensor"
    dailyOffset.metaData.name shouldBe "data.table_a__1d-1h__sensor"
  }

  it should "use the interval-only qualifier when the offset is zero" in {
    val hourMillis = 60 * 60 * 1000L
    val tableInfo = new TableInfo()
      .setTable("data.table_a")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd-HH-mm")
      .setPartitionInterval(WindowUtils.fromMillis(3 * hourMillis))
    val executionInfo = new ExecutionInfo()
      .setTableDependencies(List(new TableDependency().setTableInfo(tableInfo)).asJava)
    val metaData =
      new MetaData().setName("test_team.sq__3").setTeam("test_team").setVersion("1").setExecutionInfo(executionInfo)

    val sensor = ExternalSourceSensorUtil.sensorNodes(metaData).head
    sensor.metaData.name shouldBe "data.table_a__3h__sensor"
  }

  it should "keep the legacy name for plain daily dependencies" in {
    val tableInfo = new TableInfo()
      .setTable("data.table_a")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd")

    val executionInfo = new ExecutionInfo()
      .setTableDependencies(List(new TableDependency().setTableInfo(tableInfo)).asJava)
    val metaData =
      new MetaData().setName("test_team.gb__1").setTeam("test_team").setVersion("1").setExecutionInfo(executionInfo)

    // Daily-no-offset is the overwhelmingly common case — its sensor names must not change, or
    // every existing customer's sensors would be renamed on upgrade.
    val sensor = ExternalSourceSensorUtil.sensorNodes(metaData).head
    sensor.metaData.name shouldBe "data.table_a__sensor"
  }

  "sensorNodes" should "set correct sensor metadata for grouped dependencies" in {
    // Create multiple dependencies to test comprehensive metadata structure
    val dependencies = (1 to 3).map { i =>
      val tableInfo = new TableInfo()
        .setTable(s"data.table_$i")
        .setPartitionColumn("ds")
        .setPartitionFormat("yyyy-MM-dd")
      new TableDependency().setTableInfo(tableInfo)
    }

    val executionInfo = new ExecutionInfo()
      .setTableDependencies(dependencies.asJava)
      .setOfflineSchedule("@daily")
      .setStepDays(1)

    val baseMetaData = new MetaData()
      .setName("multi_dependency_entity")
      .setTeam("sensor_test")
      .setVersion("1")
      .setOutputNamespace("test_namespace")
      .setExecutionInfo(executionInfo)

    val sensorNodes = ExternalSourceSensorUtil.sensorNodes(baseMetaData)

    sensorNodes should have size 3

    sensorNodes.foreach { sensor =>
      // Each sensor should have empty table dependencies
      sensor.metaData.executionInfo.tableDependencies should be(empty)

      // Sensor output table should match the source table
      val expectedTable = sensor.getSourceTableDependency.tableInfo.table
      sensor.metaData.executionInfo.outputTableInfo.table should equal(expectedTable)

      // Sensor name should follow the expected pattern
      sensor.metaData.name should endWith("__sensor")

      // Verify sensor inherits team and other metadata
      sensor.metaData.team should equal("sensor_test")
      sensor.metaData.version should equal("1")
    }
  }
}
