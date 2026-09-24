package ai.chronon.api.test.planner

import ai.chronon.api.planner.{ConfPlanner, ExternalSourceSensorUtil}
import ai.chronon.api._
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.planner.{ConfPlan, ExternalSourceSensorNode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class ExternalSourceSensorUtilTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  private def semanticHash(sensor: ExternalSourceSensorNode): String = {
    val planner = new ConfPlanner[ExternalSourceSensorNode](sensor) {
      override def buildPlan: ConfPlan = throw new UnsupportedOperationException("not used by this test")
    }
    planner.toNode(sensor.metaData, _.setExternalSourceSensor(sensor), sensor).semanticHash
  }

  private def sensorFor(tableInfo: TableInfo,
                        startOffset: Window = WindowUtils.zero(),
                        endOffset: Window = WindowUtils.zero()): ExternalSourceSensorNode = {
    val dependency = new TableDependency()
      .setTableInfo(tableInfo)
      .setStartOffset(startOffset)
      .setEndOffset(endOffset)
    val executionInfo = new ExecutionInfo().setTableDependencies(List(dependency).asJava)
    val metaData = new MetaData()
      .setName("test_entity")
      .setTeam("test_team")
      .setVersion("1")
      .setExecutionInfo(executionInfo)
    ExternalSourceSensorUtil.sensorNodes(metaData).head
  }

  // sensors used to be hashed through a metaData-erasing copy; the digest strips metaData itself
  // now, so the property to hold is the hash, not the shape of some intermediate copy
  "a sensor node's semantic hash" should "not depend on its metaData" in {
    def sensorNode(name: String, team: String): ExternalSourceSensorNode =
      new ExternalSourceSensorNode()
        .setSourceTableDependency(new TableDependency().setTableInfo(new TableInfo().setTable("test_table")))
        .setMetaData(new MetaData().setName(name).setTeam(team).setVersion("1"))

    ThriftJsonCodec.semanticHexDigest(sensorNode("renamed_sensor", "other_team")) should equal(
      ThriftJsonCodec.semanticHexDigest(sensorNode("test_sensor", "test_team")))
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
        .setTableDependencies(
          List(
            new TableDependency()
              .setTableInfo(tableInfo)
              .setStartOffset(WindowUtils.zero())
              .setEndOffset(WindowUtils.zero())).asJava)
      new MetaData().setName(name).setTeam("test_team").setVersion("1").setExecutionInfo(executionInfo)
    }

    val hourMillis = 60 * 60 * 1000L
    val threeHourly =
      ExternalSourceSensorUtil.sensorNodes(metaDataFor("test_team.sq__3", 3 * hourMillis, hourMillis)).head
    val dailyOffset =
      ExternalSourceSensorUtil.sensorNodes(metaDataFor("test_team.gb__1", 24 * hourMillis, hourMillis)).head

    threeHourly.metaData.name shouldBe "data.table_a__3h-1h__sensor"
    dailyOffset.metaData.name shouldBe "data.table_a__1d-1h__sensor"
    threeHourly.metaData.name should not equal dailyOffset.metaData.name
    semanticHash(threeHourly) should not equal semanticHash(dailyOffset)
  }

  it should "use the interval-only qualifier when a sub-daily grid has no offset" in {
    val hourMillis = 60 * 60 * 1000L
    val tableInfo = new TableInfo()
      .setTable("data.table_a")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd-HH-mm")
      .setPartitionInterval(WindowUtils.fromMillis(3 * hourMillis))
    val executionInfo = new ExecutionInfo()
      .setTableDependencies(
        List(
          new TableDependency()
            .setTableInfo(tableInfo)
            .setStartOffset(WindowUtils.zero())
            .setEndOffset(WindowUtils.zero())).asJava)
    val metaData =
      new MetaData().setName("test_team.sq__3").setTeam("test_team").setVersion("1").setExecutionInfo(executionInfo)

    val sensor = ExternalSourceSensorUtil.sensorNodes(metaData).head
    sensor.metaData.name shouldBe "data.table_a__3h__sensor"
  }

  it should "keep the legacy name for plain daily dependencies with zero range offsets" in {
    val tableInfo = new TableInfo()
      .setTable("data.table_a")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd")

    val executionInfo = new ExecutionInfo()
      .setTableDependencies(
        List(
          new TableDependency()
            .setTableInfo(tableInfo)
            .setStartOffset(WindowUtils.zero())
            .setEndOffset(WindowUtils.zero())).asJava)
    val metaData =
      new MetaData().setName("test_team.gb__1").setTeam("test_team").setVersion("1").setExecutionInfo(executionInfo)

    // Daily with a zero dependency range is the overwhelmingly common case. Its sensor names must
    // not change, or every existing customer's sensors would be renamed on upgrade.
    val sensor = ExternalSourceSensorUtil.sensorNodes(metaData).head
    sensor.metaData.name shouldBe "data.table_a__sensor"
  }

  it should "keep the table identity when offsets are absent" in {
    val tableInfo = new TableInfo()
      .setTable("data.table_a")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd")
    val executionInfo = new ExecutionInfo()
      .setTableDependencies(List(new TableDependency().setTableInfo(tableInfo)).asJava)
    val metaData =
      new MetaData().setName("test_team.gb__1").setTeam("test_team").setVersion("1").setExecutionInfo(executionInfo)

    val sensor = ExternalSourceSensorUtil.sensorNodes(metaData).head

    sensor.metaData.name shouldBe "data.table_a__sensor"
    semanticHash(sensor) shouldBe semanticHash(sensorFor(tableInfo.deepCopy()))
  }

  it should "reuse one semantic identity across different dependency ranges" in {
    val tableInfo = new TableInfo()
      .setTable("data.table_a")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd-HH-mm")
      .setPartitionInterval(new Window(3, TimeUnit.HOURS))
      .setPartitionOffset(WindowUtils.Hour)

    val sevenDays = new Window(7, TimeUnit.DAYS)
    val dependencies = Seq(
      new TableDependency()
        .setTableInfo(tableInfo.deepCopy())
        .setStartOffset(sevenDays)
        .setEndOffset(WindowUtils.zero()),
      new TableDependency()
        .setTableInfo(tableInfo.deepCopy())
        .setStartOffset(sevenDays)
        .setEndOffset(WindowUtils.Day),
      new TableDependency()
        .setTableInfo(tableInfo.deepCopy())
        .setStartOffset(new Window(14, TimeUnit.DAYS))
        .setEndOffset(WindowUtils.zero()),
      new TableDependency()
        .setTableInfo(tableInfo.deepCopy())
        .setStartOffset(WindowUtils.zero())
        .setEndOffset(WindowUtils.zero())
        .setStartCutOff("2024-01-01")
        .setEndCutOff("2024-12-31")
    )
    val executionInfo = new ExecutionInfo().setTableDependencies(dependencies.asJava)
    val metaData =
      new MetaData().setName("test_team.gb__1").setTeam("test_team").setVersion("1").setExecutionInfo(executionInfo)

    val sensors = ExternalSourceSensorUtil.sensorNodes(metaData)

    sensors.map(_.metaData.name).distinct shouldBe Seq("data.table_a__3h-1h__sensor")
    sensors.map(semanticHash).distinct should have size 1
  }

  it should "share one identity across cron schedules when the table and grid match" in {
    def scheduledSensor(confName: String, schedule: String): ExternalSourceSensorNode = {
      val tableInfo = new TableInfo()
        .setTable("data.table_a")
        .setPartitionColumn("ds")
        .setPartitionFormat("yyyy-MM-dd-HH-mm")
        .setPartitionInterval(new Window(3, TimeUnit.HOURS))
        .setPartitionOffset(WindowUtils.Hour)
      val executionInfo = new ExecutionInfo()
        .setTableDependencies(List(new TableDependency().setTableInfo(tableInfo)).asJava)
        .setOfflineSchedule(schedule)
      val metaData = new MetaData()
        .setName(confName)
        .setTeam("test_team")
        .setVersion("1")
        .setExecutionInfo(executionInfo)
      ExternalSourceSensorUtil.sensorNodes(metaData).head
    }

    val onTheHour = scheduledSensor("test_team.staging", "0 */3 * * *")
    val delayed = scheduledSensor("test_team.group_by", "20 */3 * * *")

    onTheHour.metaData.name shouldBe "data.table_a__3h-1h__sensor"
    delayed.metaData.name shouldBe onTheHour.metaData.name
    semanticHash(delayed) shouldBe semanticHash(onTheHour)
  }

  it should "keep range mapping on the consumer dependency instead of the generated sensor" in {
    val tableInfo = new TableInfo()
      .setTable("data.table_a")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd")
    val dependency = new TableDependency()
      .setTableInfo(tableInfo)
      .setStartOffset(new Window(7, TimeUnit.DAYS))
      .setEndOffset(WindowUtils.Day)
      .setStartCutOff("2024-01-01")
      .setEndCutOff("2024-12-31")
    val executionInfo = new ExecutionInfo().setTableDependencies(List(dependency).asJava)
    val metaData =
      new MetaData().setName("test_team.gb__1").setTeam("test_team").setVersion("1").setExecutionInfo(executionInfo)

    val sensorDependency = ExternalSourceSensorUtil.sensorNodes(metaData).head.sourceTableDependency

    // The consumer still owns the mapping from its range to the input-table range.
    dependency.startOffset shouldBe new Window(7, TimeUnit.DAYS)
    dependency.endOffset shouldBe WindowUtils.Day
    dependency.startCutOff shouldBe "2024-01-01"
    dependency.endCutOff shouldBe "2024-12-31"

    // A sensor step is already expressed on the input table's range, so its executable copy is identity.
    sensorDependency.tableInfo shouldBe tableInfo
    sensorDependency.startOffset shouldBe WindowUtils.zero()
    sensorDependency.endOffset shouldBe WindowUtils.zero()
    sensorDependency.isSetStartCutOff shouldBe false
    sensorDependency.isSetEndCutOff shouldBe false
  }

  it should "put non-grid sensing semantics in the semantic hash instead of the name" in {
    val baseInfo = new TableInfo()
      .setTable("data.table_a")
      .setPartitionColumn("ds")
      .setPartitionFormat("yyyy-MM-dd-HH-mm")
      .setPartitionInterval(WindowUtils.Day)

    val base = sensorFor(baseInfo.deepCopy())
    val sameGridSensingVariations = Seq(
      sensorFor(baseInfo.deepCopy().setPartitionColumn("DS")),
      sensorFor(baseInfo.deepCopy().setPartitionFormat("yyyyMMddHHmm")),
      sensorFor(baseInfo.deepCopy().setTimePartitioned(true)),
      sensorFor(baseInfo.deepCopy().setIsCumulative(true)),
      sensorFor(baseInfo.deepCopy().setTriggerExpr("MAX(ds)"))
    )
    val gridVariations = Seq(
      sensorFor(baseInfo.deepCopy().setPartitionInterval(new Window(12, TimeUnit.HOURS))),
      sensorFor(baseInfo.deepCopy().setPartitionOffset(WindowUtils.Hour))
    )
    val rangeVariations = Seq(
      sensorFor(baseInfo.deepCopy(), startOffset = new Window(7, TimeUnit.DAYS)),
      sensorFor(baseInfo.deepCopy(), endOffset = WindowUtils.Day)
    )

    sameGridSensingVariations.foreach { changed =>
      changed.metaData.name shouldBe base.metaData.name
      semanticHash(changed) should not equal semanticHash(base)
    }
    sameGridSensingVariations.map(semanticHash).distinct should have size sameGridSensingVariations.size

    gridVariations.foreach { changed =>
      changed.metaData.name should not equal base.metaData.name
      semanticHash(changed) should not equal semanticHash(base)
    }

    rangeVariations.foreach { changed =>
      changed.metaData.name shouldBe base.metaData.name
      semanticHash(changed) shouldBe semanticHash(base)
    }
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

  it should "budget one partition interval of retries" in {
    def sensorFor(tableInfo: TableInfo): ExternalSourceSensorNode = {
      val executionInfo = new ExecutionInfo()
        .setTableDependencies(List(new TableDependency().setTableInfo(tableInfo)).asJava)
      val metaData = new MetaData()
        .setName("test_groupby")
        .setTeam("test_team")
        .setVersion("1")
        .setExecutionInfo(executionInfo)
      ExternalSourceSensorUtil.sensorNodes(metaData).head
    }

    // daily keeps the historical 96 x 15min = 24h budget exactly
    val daily = sensorFor(new TableInfo().setTable("data.daily").setPartitionColumn("ds"))
    daily.retryCount should equal(96L)
    daily.retryIntervalMin should equal(15L)

    // an hourly sensor is redundant with its successor after one hour
    val hourly = sensorFor(
      new TableInfo()
        .setTable("data.hourly")
        .setPartitionColumn("ds")
        .setPartitionFormat("yyyy-MM-dd-HH")
        .setPartitionInterval(new Window(1, TimeUnit.HOURS)))
    hourly.retryCount should equal(4L)
    hourly.retryIntervalMin should equal(15L)

    // fine grids shrink the poll interval so the sensor still gets a few checks
    val fifteenMin = sensorFor(
      new TableInfo()
        .setTable("data.fine")
        .setPartitionColumn("ds")
        .setPartitionFormat("yyyy-MM-dd-HH-mm")
        .setPartitionInterval(new Window(15, TimeUnit.MINUTES)))
    fifteenMin.retryIntervalMin should equal(3L)
    fifteenMin.retryCount should equal(5L)

    // coverage rounds up to at least one full interval when the poll interval doesn't divide it
    val nineMin = sensorFor(
      new TableInfo()
        .setTable("data.nine")
        .setPartitionColumn("ds")
        .setPartitionFormat("yyyy-MM-dd-HH-mm")
        .setPartitionInterval(new Window(9, TimeUnit.MINUTES)))
    nineMin.retryIntervalMin should equal(2L)
    nineMin.retryCount should equal(5L)
    nineMin.retryCount * nineMin.retryIntervalMin should be >= 9L
  }
}
