package ai.chronon.api.test.planner

import ai.chronon.api.{EngineType, ExecutionInfo, PartitionSpec, ThriftJsonCodec, Window}
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.planner.{LocalRunner, StagingQueryPlanner}
import ai.chronon.api.Builders.{MetaData, StagingQuery}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ai.chronon.api.TableDependency
import ai.chronon.api.TableInfo

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

class StagingQueryPlannerTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec = PartitionSpec.daily
  private val threeHourSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * 60 * 60 * 1000)

  it should "staging query planner plans valid confs without exceptions" in {

    val stagingQueryRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/staging_queries").getPath)

    val stagingQueryConfs = LocalRunner.parseConfs[ai.chronon.api.StagingQuery](stagingQueryRootDir.toString)

    val stagingQueryPlanners = stagingQueryConfs.map(new StagingQueryPlanner(_))

    stagingQueryPlanners
      .foreach { planner =>
        noException should be thrownBy {
          val plan = planner.buildPlan
          plan.terminalNodeNames.asScala.size should be > 0

          // Validate that no node names contain forward slashes
          plan.nodes.asScala.foreach { node =>
            val nodeName = node.metaData.name
            withClue(s"Node name '$nodeName' contains forward slash") {
              nodeName should not contain "/"
            }
          }
        }
      }
  }

  it should "staging query planner should create valid plans without exceptions" in {
    val ti = new TableInfo().setTable("hello")
    val td = new TableDependency().setTableInfo(ti)
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "testStagingQuery"),
      engineType = EngineType.SPARK,
      tableDependencies = Seq(td)
    )

    val planner = new StagingQueryPlanner(stagingQuery)

    noException should be thrownBy {
      val plan = planner.buildPlan
      plan.nodes.asScala should not be empty
      plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.BACKFILL

      // Validate that no node names contain forward slashes
      plan.nodes.asScala.foreach { node =>
        val nodeName = node.metaData.name
        withClue(s"Node name '$nodeName' contains forward slash") {
          nodeName should not contain "/"
        }
      }
    }
  }

  it should "staging query planner should preserve explicit sub-daily output partition specs" in {
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      metaData = MetaData(
        name = "subDailyStagingQuery",
        executionInfo = new ExecutionInfo().setOutputTableInfo(
          new TableInfo()
            .setPartitionColumn(threeHourSpec.column)
            .setPartitionFormat(threeHourSpec.format)
            .setPartitionInterval(WindowUtils.fromMillis(threeHourSpec.spanMillis))
        )
      ),
      engineType = EngineType.SPARK
    )

    val plan = new StagingQueryPlanner(stagingQuery).buildPlan
    val node = plan.nodes.asScala.find(_.content.isSetStagingQuery).get

    node.metaData.executionInfo.outputTableInfo.partitionFormat should equal(threeHourSpec.format)
    node.metaData.executionInfo.outputTableInfo.partitionInterval should equal(WindowUtils.fromMillis(threeHourSpec.spanMillis))
  }

  it should "staging query planner should treat undeclared dependencies as daily under sub-daily outputs" in {
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      metaData = MetaData(
        name = "subDailyStagingQueryWithDailyDep",
        executionInfo = new ExecutionInfo().setOutputTableInfo(
          new TableInfo()
            .setPartitionColumn(threeHourSpec.column)
            .setPartitionFormat(threeHourSpec.format)
            .setPartitionInterval(WindowUtils.fromMillis(threeHourSpec.spanMillis))
        )
      ),
      engineType = EngineType.SPARK,
      tableDependencies = Seq(new TableDependency().setTableInfo(new TableInfo().setTable("test.undeclared_dep")))
    )

    val plan = new StagingQueryPlanner(stagingQuery).buildPlan
    val depInfo = plan.nodes.asScala.head.metaData.executionInfo.tableDependencies.asScala.head.tableInfo
    depInfo.partitionFormat should equal(PartitionSpec.daily.format)
    depInfo.partitionInterval should equal(WindowUtils.Day)
  }

  it should "staging query planner should reject partially declared dependencies under sub-daily outputs" in {
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
      metaData = MetaData(
        name = "subDailyStagingQueryWithPartialDep",
        executionInfo = new ExecutionInfo().setOutputTableInfo(
          new TableInfo()
            .setPartitionColumn(threeHourSpec.column)
            .setPartitionFormat(threeHourSpec.format)
            .setPartitionInterval(WindowUtils.fromMillis(threeHourSpec.spanMillis))
        )
      ),
      engineType = EngineType.SPARK,
      tableDependencies = Seq(
        new TableDependency().setTableInfo(new TableInfo().setTable("test.partial_dep").setPartitionColumn("event_date"))
      )
    )

    val error = the[IllegalArgumentException] thrownBy new StagingQueryPlanner(stagingQuery).buildPlan
    error.getMessage should include("test.partial_dep")
    error.getMessage should include("partition_interval")
  }

  it should "staging query planner should avoid metadata when computing semantic hash" in {
    val firstStagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "firstStagingQuery"),
      engineType = EngineType.SPARK
    )

    val secondStagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "secondStagingQuery"),
      engineType = EngineType.SPARK
    )

    val firstPlanner = new StagingQueryPlanner(firstStagingQuery)
    val secondPlanner = new StagingQueryPlanner(secondStagingQuery)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "staging query planner should create correct terminal node names" in {
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "testStagingQuery"),
      engineType = EngineType.SPARK
    )

    val planner = new StagingQueryPlanner(stagingQuery)
    val plan = planner.buildPlan

    plan.terminalNodeNames.asScala should contain key ai.chronon.planner.Mode.BACKFILL
    plan.terminalNodeNames.asScala(ai.chronon.planner.Mode.BACKFILL) should equal("testStagingQuery__staging")
  }

  it should "staging query planner should handle setups correctly" in {
    val stagingQueryWithSetups = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "testStagingQuery"),
      setups = Seq("CREATE TEMP VIEW temp_table AS SELECT 1 as id", "SET spark.sql.adaptive.enabled=true"),
      engineType = EngineType.SPARK
    )

    val planner = new StagingQueryPlanner(stagingQueryWithSetups)

    noException should be thrownBy {
      val plan = planner.buildPlan
      plan.nodes.asScala should have size 1
    }
  }

  it should "staging query planner should use default step days of 1" in {
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "testStagingQuery"),
      engineType = EngineType.SPARK
    )

    val planner = new StagingQueryPlanner(stagingQuery)
    val plan = planner.buildPlan

    // Should create plan successfully with default step days
    plan.nodes.asScala should have size 1
    val node = plan.nodes.asScala.head
    node.metaData should not be null
    node.metaData.name should equal("testStagingQuery__staging")
  }

  it should "staging query planner should propagate stepDays from executionInfo" in {
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(
        name = "testStagingQuery",
        executionInfo = new ExecutionInfo().setStepDays(7)
      ),
      engineType = EngineType.SPARK
    )

    val planner = new StagingQueryPlanner(stagingQuery)
    val plan = planner.buildPlan

    val node = plan.nodes.asScala.head
    node.metaData.executionInfo.stepDays should equal(7)
  }

  it should "staging query planner should default stepDays to 1 when executionInfo has no stepDays" in {
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(
        name = "testStagingQuery",
        executionInfo = new ExecutionInfo()
      ),
      engineType = EngineType.SPARK
    )

    val planner = new StagingQueryPlanner(stagingQuery)
    val plan = planner.buildPlan

    val node = plan.nodes.asScala.head
    node.metaData.executionInfo.stepDays should equal(1)
  }

  it should "staging query planner should default stepDays to 1 when no executionInfo" in {
    val stagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(name = "testStagingQuery"),
      engineType = EngineType.SPARK
    )

    val planner = new StagingQueryPlanner(stagingQuery)
    val plan = planner.buildPlan

    val node = plan.nodes.asScala.head
    node.metaData.executionInfo.stepDays should equal(1)
  }

  it should "staging query planner should produce same semantic hash with different executionInfo in metadata" in {
    val firstStagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(
        name = "testStagingQuery1",
        executionInfo = new ExecutionInfo().setStepDays(2)
      ),
      engineType = EngineType.SPARK
    )

    val secondStagingQuery = StagingQuery(
      query = "SELECT * FROM test_table",
      metaData = MetaData(
        name = "testStagingQuery2",
        executionInfo = new ExecutionInfo().setStepDays(5)
      ),
      engineType = EngineType.SPARK
    )

    val firstPlanner = new StagingQueryPlanner(firstStagingQuery)
    val secondPlanner = new StagingQueryPlanner(secondStagingQuery)

    val firstPlan = firstPlanner.buildPlan
    val secondPlan = secondPlanner.buildPlan

    // Semantic hashes should be identical since metadata (including executionInfo) is excluded
    val firstSemanticHashes = firstPlan.nodes.asScala.map(_.semanticHash)
    val secondSemanticHashes = secondPlan.nodes.asScala.map(_.semanticHash)
    firstSemanticHashes should equal(secondSemanticHashes)
  }

  it should "keep sensor identity for the same table and grid while hashing partition column changes" in {
    def plannedSensor(partitionColumn: String) = {
      val tableInfo = new TableInfo()
        .setTable("rain_focus.fact_rf_attendee_snp_hrl")
        .setPartitionColumn(partitionColumn)
        .setPartitionFormat("yyyy-MM-dd")
        .setPartitionInterval(WindowUtils.Day)
      val dependency = new TableDependency()
        .setTableInfo(tableInfo)
        .setStartOffset(WindowUtils.zero())
        .setEndOffset(WindowUtils.zero())
      val stagingQuery = StagingQuery(
        query = "SELECT * FROM rain_focus.fact_rf_attendee_snp_hrl",
        metaData = MetaData(name = s"rain_focus.object_$partitionColumn"),
        engineType = EngineType.SPARK,
        tableDependencies = Seq(dependency)
      )

      new StagingQueryPlanner(stagingQuery).buildPlan.nodes.asScala.find(_.content.isSetExternalSourceSensor).get
    }

    val lowerCase = plannedSensor("ds")
    val upperCase = plannedSensor("DS")

    lowerCase.metaData.name shouldBe "rain_focus.fact_rf_attendee_snp_hrl__sensor"
    upperCase.metaData.name shouldBe lowerCase.metaData.name
    upperCase.semanticHash should not equal lowerCase.semanticHash
    ThriftJsonCodec.hexDigest(upperCase) should not equal ThriftJsonCodec.hexDigest(lowerCase)
    lowerCase.content.getExternalSourceSensor.sourceTableDependency.tableInfo.partitionColumn shouldBe "ds"
    upperCase.content.getExternalSourceSensor.sourceTableDependency.tableInfo.partitionColumn shouldBe "DS"
  }

  it should "share sensor semantics across dependency range changes" in {
    def plannedSensor(startOffset: Window, endOffset: Window) = {
      val tableInfo = new TableInfo()
        .setTable("data.events")
        .setPartitionColumn("ds")
        .setPartitionFormat("yyyy-MM-dd")
        .setPartitionInterval(WindowUtils.Day)
      val dependency = new TableDependency()
        .setTableInfo(tableInfo)
        .setStartOffset(startOffset)
        .setEndOffset(endOffset)
      val stagingQuery = StagingQuery(
        query = "SELECT * FROM data.events",
        metaData = MetaData(name = s"events_${startOffset.length}_${endOffset.length}"),
        engineType = EngineType.SPARK,
        tableDependencies = Seq(dependency)
      )

      new StagingQueryPlanner(stagingQuery).buildPlan.nodes.asScala.find(_.content.isSetExternalSourceSensor).get
    }

    val currentDay = plannedSensor(WindowUtils.zero(), WindowUtils.zero())
    val sevenDayLookback = plannedSensor(new Window(7, ai.chronon.api.TimeUnit.DAYS), WindowUtils.zero())
    val oneDayLag = plannedSensor(WindowUtils.zero(), WindowUtils.Day)
    val sensors = Seq(currentDay, sevenDayLookback, oneDayLag)

    sensors.map(_.metaData.name).distinct shouldBe Seq("data.events__sensor")
    sensors.map(ThriftJsonCodec.hexDigest(_)).distinct should have size sensors.size
    sensors.map(_.semanticHash).distinct should have size 1
  }

  it should "staging query planner should produce exactly one node wrapping a StagingQuery for canary confs" in {
    val stagingQueryRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/staging_queries").getPath)

    val stagingQueryConfs = LocalRunner.parseConfs[ai.chronon.api.StagingQuery](stagingQueryRootDir.toString)

    stagingQueryConfs.foreach { stagingQueryConf =>
      val planner = new StagingQueryPlanner(stagingQueryConf)
      val plan = planner.buildPlan

      val node = plan.nodes.asScala.head
      // Node should have content
      node.content should not be null
      // Content should have a stagingQuery set
      node.content.getStagingQuery should not be null
      // The wrapped staging query should not be null
      node.content.getStagingQuery.stagingQuery should not be null
    }
  }
}
