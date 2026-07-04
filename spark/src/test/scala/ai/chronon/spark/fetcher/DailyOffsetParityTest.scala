package ai.chronon.spark.fetcher

import ai.chronon.api._
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.batch.ModularMonolith
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{MockApi, OnlineUtils, SparkTestBase}
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers

import java.util.TimeZone
import java.util.concurrent.Executors

import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}

/** Offline/online parity for a DAILY join grid with a sub-daily offset: 1d interval, 1h offset
  * (partitions start at 01:00 UTC and hold [01:00, 01:00 next day); ds = interval start, labeled
  * yyyy-MM-dd-HH-mm). This is the compute-reuse scenario that motivated daily-with-offset: a
  * daily-cadence consumer sitting directly on a 3h+1h upstream grid instead of forcing every
  * downstream onto the sub-daily cadence.
  *
  * Cells (all under the one 1d+1h join):
  *  - tmp    TEMPORAL/EVENTS on the 3h+1h grid — sawtooth windows at left.ts; the 14d window
  *           locks the SawtoothMutationAggregator daily-tail-hop fix for a 01:00 (non-midnight)
  *           daily batch end: the 10000 @ 2023-07-31 00:30 event sits between the tail-hop
  *           boundary (07-31 00:00) and raw batchEnd - 14d (07-31 01:00).
  *  - snap3  SNAPSHOT/EVENTS on the 3h+1h grid — per-row pick as of floor(left.ts, 3h+1h).
  *  - snapd1 SNAPSHOT/EVENTS whose OUTPUT grid is 1d+1h over the SAME 3h+1h source — per-row
  *           pick as of floor(left.ts, 1d+1h); the flagship "daily consumer of a sub-daily
  *           upstream" cell.
  *  - entd1  SNAPSHOT/ENTITIES on the 1d+1h grid — per-row pick of the latest snapshot
  *           partition complete at floor(left.ts, 1d+1h).
  *
  * The discriminating left row is u1 @ 2023-08-14 00:30: after midnight but before the 01:00
  * boundary, so it belongs to left partition 2023-08-13-01-00 and its daily-offset picks read
  * one partition further back than a midnight-daily grid would.
  *
  * Online phase: batch end pinned at the 2023-08-14 01:00 boundary (every groupBy translates the
  * join boundary onto its own grid; 01:00 is a boundary of both grids here). SNAPSHOT cells serve
  * the batch value as of that batch end, so for the mid-partition row (u1 @ 12:07) the sub-daily
  * snap3 cell online-serves the 01:00 state while offline backfill picked 10:00 — that bounded
  * staleness is the documented cross-grid convention and is asserted explicitly via overrides.
  * Cells whose grid equals the join grid (snapd1, entd1) have exact online/offline parity.
  */
class DailyOffsetParityTest extends SparkTestBase with Matchers {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  // 1d interval, 1h offset: partition boundaries at 01:00 UTC
  private val dailyOffsetSpec =
    PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 24 * WindowUtils.Hour.millis, WindowUtils.Hour.millis)
  // 3h interval, 1h offset: boundaries 01:00, 04:00, ..., 22:00 — every 1d+1h boundary is on it
  private val subDailySpec =
    PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * WindowUtils.Hour.millis, WindowUtils.Hour.millis)

  private val sixHours = new Window(6, TimeUnit.HOURS)
  private val oneDay = new Window(1, TimeUnit.DAYS)
  private val fourteenDays = new Window(14, TimeUnit.DAYS)

  private def ts(arg: String): Long = TsUtils.datetimeToTs(s"$arg:00")

  // Left rows. floor(ts, 1d+1h): 12:07 and exactly-01:00 land in 2023-08-14-01-00; the 00:30
  // row is pre-boundary and lands in 2023-08-13-01-00.
  private val T_U1_MID_PARTITION = ts("2023-08-14 12:07")
  private val T_U1_PRE_BOUNDARY = ts("2023-08-14 00:30")
  private val T_U2_ON_BOUNDARY = ts("2023-08-14 01:00")

  case class Golden(user: String,
                    tsMillis: Long,
                    leftDs: String,
                    features: Map[String, Any],
                    // online expectations that legitimately differ from offline: SNAPSHOT cells
                    // on a finer grid than the join serve as of the upload batch end, not per row
                    onlineOverrides: Map[String, Any] = Map.empty)

  private val goldens = Seq(
    // u1 @ 12:07 — mid-partition row of 2023-08-14-01-00.
    Golden(
      "u1",
      T_U1_MID_PARTITION,
      "2023-08-14-01-00",
      Map(
        // 1d sawtooth ending 12:07 (hourly tail hop -> 2023-08-13 12:00): 3 @ 08-13 20:00 +
        // 5 @ 08:15 + 11 @ 11:30 = 19; the 1000 @ 08-13 11:00 is just outside the tail.
        "tmp_user_id_txn_amount_sum_1d" -> 19L,
        // 14d with DAILY tail hops: tail = floor(12:07 - 14d, 1d) = 2023-07-31 00:00, so the
        // 10000 @ 07-31 00:30 hop-gap event IS in: 10000 + 1000 + 3 + 5 + 11 = 11019.
        "tmp_user_id_txn_amount_sum_14d" -> 11019L,
        // snap3 as-of floor(12:07, 3h+1h) = 10:00: 8 @ 08-13 19:30 + 15 @ 23:40 + 20 @ 02:30 +
        // 100 @ 08:30 = 143 (ts < bound; nothing at/after 10:00 for u1).
        "snap3_user_id_amount_3h_sum" -> 143L,
        // 6h window [04:00, 10:00): 100 @ 08:30.
        "snap3_user_id_amount_3h_sum_6h" -> 100L,
        // snapd1 as-of floor(12:07, 1d+1h) = 2023-08-14 01:00: 8 @ 19:30 + 15 @ 23:40 = 23
        // (20 @ 02:30 and 100 @ 08:30 are at/after the 01:00 bound).
        "snapd1_user_id_amount_3h_sum" -> 23L,
        // entd1: floor(12:07, 1d+1h) = 08-14 01:00 -> latest partition complete by then is
        // 2023-08-13-01-00 -> u1 balance 102.
        "entd1_user_id_balance_d1" -> 102L
      ),
      // online batch end 01:00: snap3 serves the 01:00 state (8 + 15 = 23; 6h window
      // [2023-08-13 19:00, 01:00): 8 @ 19:30 + 15 @ 23:40 = 23)
      onlineOverrides = Map(
        "snap3_user_id_amount_3h_sum" -> 23L,
        "snap3_user_id_amount_3h_sum_6h" -> 23L
      )
    ),
    // u1 @ 00:30 — post-midnight but pre-01:00: left partition 2023-08-13-01-00, and every
    // daily-offset pick reads one partition further back than a midnight grid would.
    // Offline-golden-only: 00:30 is before the phase batch end (01:00).
    Golden(
      "u1",
      T_U1_PRE_BOUNDARY,
      "2023-08-13-01-00",
      Map(
        // 1d sawtooth ending 08-14 00:30 (hourly tail hop -> 08-13 00:00): 1000 @ 08-13 11:00 +
        // 3 @ 08-13 20:00 = 1003.
        "tmp_user_id_txn_amount_sum_1d" -> 1003L,
        // 14d tail = floor(00:30 - 14d, 1d) = 07-31 00:00: 10000 + 1000 + 3 = 11003.
        "tmp_user_id_txn_amount_sum_14d" -> 11003L,
        // snap3 as-of floor(00:30, 3h+1h) = 2023-08-13 22:00: only 8 @ 19:30 (15 @ 23:40 is
        // at/after the bound).
        "snap3_user_id_amount_3h_sum" -> 8L,
        // 6h window [16:00, 22:00) of 08-13: 8 @ 19:30.
        "snap3_user_id_amount_3h_sum_6h" -> 8L,
        // snapd1 as-of floor(00:30, 1d+1h) = 2023-08-13 01:00: no u1 events before that -> null.
        "snapd1_user_id_amount_3h_sum" -> null,
        // entd1: floor = 08-13 01:00 -> partition 2023-08-12-01-00 -> 101.
        "entd1_user_id_balance_d1" -> 101L
      )
    ),
    // u2 @ exactly 01:00 — on the 1d+1h boundary; also equals the phase batch end, so every
    // cell has exact online/offline parity (a query at exactly batch end is legal).
    Golden(
      "u2",
      T_U2_ON_BOUNDARY,
      "2023-08-14-01-00",
      Map(
        // 1d sawtooth ending 01:00 (hourly tail hop -> 08-13 01:00): 7 @ 08-14 00:40.
        "tmp_user_id_txn_amount_sum_1d" -> 7L,
        "tmp_user_id_txn_amount_sum_14d" -> 7L,
        // snap3 as-of floor(01:00, 3h+1h) = 01:00 (on-grid): 40 @ 08-13 21:00 + 70 @ 00:45 = 110.
        "snap3_user_id_amount_3h_sum" -> 110L,
        // 6h window [2023-08-13 19:00, 01:00): both events inside -> 110.
        "snap3_user_id_amount_3h_sum_6h" -> 110L,
        // snapd1 as-of floor(01:00, 1d+1h) = 01:00: same two events -> 110.
        "snapd1_user_id_amount_3h_sum" -> 110L,
        // entd1: floor = 08-14 01:00 -> partition 2023-08-13-01-00 -> 202.
        "entd1_user_id_balance_d1" -> 202L
      )
    )
  )

  it should "match goldens offline and online for a 1d+1h join over 3h+1h and 1d+1h cells" in {
    val namespace = "daily_offset_parity"
    val joinConf = generateJoin(namespace, spark)
    implicit val tableUtils: TableUtils = TableUtils(spark, dailyOffsetSpec)

    val dateRange = new DateRange().setStartDate("2023-08-13-01-00").setEndDate("2023-08-14-01-00")
    ModularMonolith.run(joinConf, dateRange)

    withClue("1d+1h join output must land on offset-boundary yyyy-MM-dd-HH-mm partitions: ") {
      tableUtils.partitions(joinConf.metaData.outputTable).toSet shouldEqual
        Set("2023-08-13-01-00", "2023-08-14-01-00")
    }

    val offlineRows = assertOfflineMatchesGoldens(joinConf, goldens)

    // Online: batch end pinned at the 2023-08-14 01:00 boundary. Servable rows are those at or
    // after the batch end: u1 @ 12:07 and u2 @ exactly 01:00. The 00:30 row is offline-only.
    val phaseRows = goldens.filter(_.leftDs == "2023-08-14-01-00")
    serveAndAssertOnline(joinConf, "2023-08-14-01-00", dailyOffsetSpec, namespace, phaseRows, offlineRows)
  }

  // ---------------------------------------------------------------------------------------------
  // Assertion harness (mirrors JoinParityMatrixTest, plus per-row online overrides)
  // ---------------------------------------------------------------------------------------------

  private def assertOfflineMatchesGoldens(joinConf: Join, goldens: Seq[Golden])(implicit
      tableUtils: TableUtils): Map[(String, Long), Map[String, Any]] = {
    val outputDf = tableUtils.sql(s"SELECT * FROM ${joinConf.metaData.outputTable}")
    outputDf.show(truncate = false)
    val featureColumns = goldens.flatMap(_.features.keys).distinct

    val collectedRows = outputDf.collect()
    withClue(s"offline output of ${joinConf.metaData.outputTable} should have one row per golden: ") {
      collectedRows.length shouldEqual goldens.length
    }

    val offlineRows: Map[(String, Long), Map[String, Any]] = collectedRows
      .map { row =>
        val key = (row.getAs[String]("user_id"), row.getAs[Long]("ts"))
        val values: Map[String, Any] =
          (featureColumns.map(c => c -> row.getAs[Any](c)) :+ ("ds" -> row.getAs[String]("ds"))).toMap
        key -> values
      }
      .toMap

    withClue("offline output keys should equal golden keys: ") {
      offlineRows.keySet shouldEqual goldens.map(g => (g.user, g.tsMillis)).toSet
    }

    goldens.foreach { golden =>
      val actual = offlineRows((golden.user, golden.tsMillis))
      withClue(s"offline left partition ds for ${golden.user} @ ${TsUtils.toStr(golden.tsMillis)}: ") {
        actual("ds") shouldEqual golden.leftDs
      }
      golden.features.foreach { case (feature, expected) =>
        withClue(s"offline $feature for ${golden.user} @ ${TsUtils.toStr(golden.tsMillis)}: ") {
          actual(feature) shouldEqual expected
        }
      }
    }
    offlineRows
  }

  private def serveAndAssertOnline(joinConf: Join,
                                   endDs: String,
                                   joinSpec: PartitionSpec,
                                   namespace: String,
                                   goldens: Seq[Golden],
                                   offlineRows: Map[(String, Long), Map[String, Any]]): Unit = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("DailyOffsetParityTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    joinConf.joinParts.toScala.foreach { jp =>
      val groupBySpec = jp.groupBy.partitionSpec(joinSpec)
      val groupByEndDs = joinSpec.translate(endDs, groupBySpec)
      spark.sql(s"DROP TABLE IF EXISTS ${jp.groupBy.metaData.uploadTable}")
      val groupByTableUtils = TableUtils(spark, groupBySpec)
      OnlineUtils.serve(groupByTableUtils,
                        inMemoryKvStore,
                        kvStoreFunc,
                        namespace,
                        groupByEndDs,
                        jp.groupBy,
                        dropDsOnWrite = true)
    }

    inMemoryKvStore.create(MetadataDataset)
    new MetadataStore(FetchContext(inMemoryKvStore)).putJoinConf(joinConf)

    val fetcher = mockApi.buildFetcher(debug = true)
    val requests = goldens.map { g =>
      Request(joinConf.metaData.name, Map("user_id" -> (g.user: AnyRef)), Some(g.tsMillis))
    }
    val responses = Await.result(fetcher.fetchJoin(requests), Duration(100, SECONDS))

    responses.zip(goldens).foreach { case (response, golden) =>
      val fetched = response.values.get
      val expectedOnline = golden.features ++ golden.onlineOverrides
      expectedOnline.foreach { case (feature, expected) =>
        withClue(s"online $feature for ${golden.user} @ ${TsUtils.toStr(golden.tsMillis)} " +
          s"(batch end $endDs), full response: $fetched : ") {
          fetched.get(feature).orNull shouldEqual expected
          if (!golden.onlineOverrides.contains(feature)) {
            fetched.get(feature).orNull shouldEqual offlineRows((golden.user, golden.tsMillis))(feature)
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Join fixture
  // ---------------------------------------------------------------------------------------------
  private def generateJoin(namespace: String, spark: SparkSession): Join = {
    SparkTestBase.createDatabase(spark, namespace)

    // Left events on the 1d+1h grid. The 00:30 row belongs to the PREVIOUS day's partition.
    val leftTable = s"$namespace.left_events"
    spark
      .createDataFrame(
        Seq(
          ("u1", T_U1_MID_PARTITION, "2023-08-14-01-00"),
          ("u1", T_U1_PRE_BOUNDARY, "2023-08-13-01-00"),
          ("u2", T_U2_ON_BOUNDARY, "2023-08-14-01-00")
        ))
      .toDF("user_id", "ts", "ds")
      .save(leftTable)

    // TEMPORAL/EVENTS source on the 3h+1h grid. Events at/after the 01:00 batch end reach the
    // fetcher via streaming injection (ds >= endDs partitions). The 10000 @ 2023-07-31 00:30
    // is the daily-tail-hop-gap lock for the 01:00 batch end (see class doc).
    val txnTable = s"$namespace.txn_events"
    spark
      .createDataFrame(
        Seq(
          ("u1", 10000L, ts("2023-07-31 00:30"), "2023-07-30-22-00"), // hop-gap event
          ("u1", 1000L, ts("2023-08-13 11:00"), "2023-08-13-10-00"),
          ("u1", 3L, ts("2023-08-13 20:00"), "2023-08-13-19-00"),
          ("u2", 7L, ts("2023-08-14 00:40"), "2023-08-13-22-00"), // batch side of the 01:00 end
          ("u1", 5L, ts("2023-08-14 08:15"), "2023-08-14-07-00"), // post-batch-end: streaming only
          ("u1", 11L, ts("2023-08-14 11:30"), "2023-08-14-10-00"), // post-batch-end: streaming only
          // u9 is never queried; keeps the streaming injection non-empty at the range tail
          ("u9", 999L, ts("2023-08-14 13:40"), "2023-08-14-13-00")
        ))
      .toDF("user_id", "txn_amount", "ts", "ds")
      .save(txnTable)

    // SNAPSHOT/EVENTS source on the 3h+1h grid with real timestamps; consumed by BOTH snap3
    // (3h+1h output grid) and snapd1 (1d+1h output grid).
    val offsetEventsTable = s"$namespace.offset_grid_events"
    spark
      .createDataFrame(
        Seq(
          ("u1", 8L, ts("2023-08-13 19:30"), "2023-08-13-19-00"),
          ("u2", 40L, ts("2023-08-13 21:00"), "2023-08-13-19-00"),
          ("u1", 15L, ts("2023-08-13 23:40"), "2023-08-13-22-00"),
          ("u2", 70L, ts("2023-08-14 00:45"), "2023-08-13-22-00"),
          ("u1", 20L, ts("2023-08-14 02:30"), "2023-08-14-01-00"),
          ("u1", 100L, ts("2023-08-14 08:30"), "2023-08-14-07-00")
        ))
      .toDF("user_id", "amount_3h", "ts", "ds")
      .save(offsetEventsTable)

    // SNAPSHOT/ENTITIES source on the 1d+1h grid: partition P holds state for [P, P+1d);
    // balances encode the partition so a wrong pick is unambiguous.
    val dailyOffsetBalanceTable = s"$namespace.daily_offset_balance"
    spark
      .createDataFrame(
        Seq(
          ("u1", 101L, "2023-08-12-01-00"),
          ("u2", 201L, "2023-08-12-01-00"),
          ("u1", 102L, "2023-08-13-01-00"),
          ("u2", 202L, "2023-08-13-01-00"),
          ("u1", 103L, "2023-08-14-01-00"),
          ("u2", 203L, "2023-08-14-01-00")
        ))
      .toDF("user_id", "balance_d1", "ds")
      .save(dailyOffsetBalanceTable)

    val temporalEventsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "daily_offset_txn_sum",
                                   executionInfo = executionInfo(subDailySpec)),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "txn_amount"),
                           timeColumn = "ts",
                           startPartition = "2023-07-30-22-00"),
            subDailySpec
          ),
          table = txnTable,
          topic = "daily_offset_txn_topic"
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders
          .Aggregation(operation = Operation.SUM, inputColumn = "txn_amount", windows = Seq(oneDay, fourteenDays))),
      accuracy = Accuracy.TEMPORAL
    )

    val subDailySnapshotEventsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "daily_offset_snap3_amount",
                                   executionInfo = executionInfo(subDailySpec)),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "amount_3h"),
                           timeColumn = "ts",
                           startPartition = "2023-08-13-19-00"),
            subDailySpec
          ),
          table = offsetEventsTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_3h",
                             windows = Seq(WindowUtils.Unbounded, sixHours))),
      accuracy = Accuracy.SNAPSHOT
    )

    // The flagship cell: OUTPUT grid 1d+1h, source grid 3h+1h (boundary-aligned at 01:00).
    val dailyOffsetSnapshotEventsGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "daily_offset_snapd1_amount",
                                   executionInfo = executionInfo(dailyOffsetSpec)),
      sources = Seq(
        Builders.Source.events(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "amount_3h"),
                           timeColumn = "ts",
                           startPartition = "2023-08-13-19-00"),
            subDailySpec
          ),
          table = offsetEventsTable
        )),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.SUM,
                             inputColumn = "amount_3h",
                             windows = Seq(WindowUtils.Unbounded))),
      accuracy = Accuracy.SNAPSHOT
    )

    val dailyOffsetSnapshotEntitiesGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "daily_offset_balance_d1",
                                   executionInfo = executionInfo(dailyOffsetSpec)),
      sources = Seq(
        Builders.Source.entities(
          query = withPartition(
            Builders.Query(selects = Builders.Selects("user_id", "balance_d1"),
                           startPartition = "2023-08-12-01-00"),
            dailyOffsetSpec
          ),
          snapshotTable = dailyOffsetBalanceTable
        )),
      keyColumns = Seq("user_id")
      // no aggregations: pass-through latest state per partition (inferred SNAPSHOT accuracy)
    )

    Builders.Join(
      left = Builders.Source.events(
        query = withPartition(
          Builders.Query(selects = Builders.Selects("user_id", "ts"), startPartition = "2023-08-13-01-00"),
          dailyOffsetSpec
        ),
        table = leftTable
      ),
      joinParts = Seq(
        Builders.JoinPart(groupBy = temporalEventsGroupBy, prefix = "tmp").setUseLongNames(false),
        Builders.JoinPart(groupBy = subDailySnapshotEventsGroupBy, prefix = "snap3").setUseLongNames(false),
        Builders.JoinPart(groupBy = dailyOffsetSnapshotEventsGroupBy, prefix = "snapd1").setUseLongNames(false),
        Builders.JoinPart(groupBy = dailyOffsetSnapshotEntitiesGroupBy, prefix = "entd1").setUseLongNames(false)
      ),
      metaData = Builders.MetaData(namespace = namespace,
                                   name = "daily_offset_parity_join",
                                   team = "chronon",
                                   executionInfo = executionInfo(dailyOffsetSpec))
    )
  }

  // ---------------------------------------------------------------------------------------------
  // Builders helpers
  // ---------------------------------------------------------------------------------------------

  // Declares the partition grid on a source query. The offset is always set explicitly: an unset
  // query offset falls back to the JOIN's offset, which would silently move a source's grid.
  private def withPartition(query: Query, partitionSpec: PartitionSpec): Query =
    query
      .setPartitionColumn(partitionSpec.column)
      .setPartitionFormat(partitionSpec.format)
      .setPartitionInterval(partitionSpec.intervalWindow)
      .setPartitionOffset(offsetWindow(partitionSpec))

  private def executionInfo(partitionSpec: PartitionSpec): ExecutionInfo =
    new ExecutionInfo()
      .setOutputTableInfo(
        new TableInfo()
          .setPartitionColumn(partitionSpec.column)
          .setPartitionFormat(partitionSpec.format)
          .setPartitionInterval(partitionSpec.intervalWindow)
          .setPartitionOffset(offsetWindow(partitionSpec))
      )

  private def offsetWindow(partitionSpec: PartitionSpec): Window =
    new Window((partitionSpec.offsetMillis / WindowUtils.Hour.millis).toInt, TimeUnit.HOURS)
}
