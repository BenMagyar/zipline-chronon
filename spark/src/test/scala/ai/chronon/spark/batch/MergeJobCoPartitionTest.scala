package ai.chronon.spark.batch

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.planner.JoinMergeNode
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.SparkTestBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions._
import org.junit.Assert._

class MergeJobCoPartitionTest extends SparkTestBase {

  // fixed partition count and no AQE so the exchange count in the physical plan is deterministic
  override protected def sparkConfs: Map[String, String] = Map(
    "spark.sql.shuffle.partitions" -> "4",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1"
  )

  private implicit val tableUtils: TableUtils = TableUtils(spark)
  private val ds = "2024-03-01"
  private val namespace = "test_namespace_merge_copartition"

  private def groupBy(name: String, key: String, accuracy: Accuracy): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(table = s"$namespace.${name}_events",
                               query = Builders.Query(selects = Builders.Selects("amount")))),
      keyColumns = Seq(key),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "amount")),
      accuracy = accuracy,
      metaData = Builders.MetaData(name = s"copart.$name", namespace = namespace, team = "test_team")
    )

  private def partTable(key: String, valueCol: String, rows: Seq[(String, Long, Double)]): DataFrame = {
    import spark.implicits._
    rows.toDF(key, "ts", valueCol).withColumn("ds", lit(ds))
  }

  private def exchanges(df: DataFrame): Int =
    df.queryExecution.executedPlan.collect { case e: ShuffleExchangeExec => e }.size

  private def sortedRows(df: DataFrame): Seq[String] =
    df.orderBy("user", "item", "ts").collect().map(_.mkString("|")).toSeq

  it should "shuffle the left once across temporal parts and keep results identical" in {
    import spark.implicits._

    val userPart = Builders.JoinPart(groupBy = groupBy("user_sum", "user", Accuracy.TEMPORAL))
    val itemPart = Builders.JoinPart(groupBy = groupBy("item_sum", "item", Accuracy.TEMPORAL))
    val sellerPart = Builders.JoinPart(groupBy = groupBy("seller_sum", "seller", Accuracy.TEMPORAL),
                                       keyMapping = Map("item" -> "seller"))
    val snapshotPart = Builders.JoinPart(groupBy = groupBy("user_snap", "user", Accuracy.SNAPSHOT))
    val parts = Seq(snapshotPart, userPart, itemPart, sellerPart)

    val join = Builders.Join(
      left = Builders.Source.events(table = s"$namespace.left", query = Builders.Query()),
      joinParts = parts,
      metaData = Builders.MetaData(name = "copart.join", namespace = namespace, team = "test_team")
    )
    val mergeJob = new MergeJob(new JoinMergeNode().setJoin(join),
                                Builders.MetaData(name = "copart.join__merged", namespace = namespace),
                                new DateRange().setStartDate(ds).setEndDate(ds),
                                parts)

    val left = Seq(("u1", "i1", 1000L), ("u1", "i2", 2000L), ("u2", "i1", 3000L), ("u3", "i3", 4000L))
      .toDF("user", "item", "ts")
      .withColumn("ds", lit(ds))
      .withColumn(Constants.TimePartitionColumn, lit(ds))
    val rightParts = Seq(
      snapshotPart -> partTable("user", "amount_sum", Seq(("u1", 0L, 1.0), ("u2", 0L, 2.0))),
      userPart -> partTable("user", "amount_sum", Seq(("u1", 1000L, 10.0), ("u1", 2000L, 11.0), ("u2", 3000L, 12.0))),
      itemPart -> partTable("item", "amount_sum", Seq(("i1", 1000L, 20.0), ("i2", 2000L, 21.0), ("i3", 4000L, 22.0))),
      sellerPart -> partTable("seller", "amount_sum", Seq(("i1", 3000L, 30.0), ("i3", 4000L, 31.0)))
    )

    spark.conf.set("spark.chronon.join.merge.copartition_on_ts", "false")
    val chained = mergeJob.joinAllParts(left, rightParts)
    spark.conf.set("spark.chronon.join.merge.copartition_on_ts", "true")
    val coPartitioned = mergeJob.joinAllParts(left, rightParts)

    // temporal parts: one exchange for the left plus one per part; the snapshot part joins on ts_ds so it
    // re-shuffles both sides once more
    val temporal = parts.count(_.groupBy.inferredAccuracy == Accuracy.TEMPORAL)
    val snapshot = parts.size - temporal
    assertEquals(1 + temporal + 2 * snapshot, exchanges(coPartitioned))
    // the plain chain re-shuffles the widening left for every part
    assertTrue(s"expected the plain chain to shuffle more, got ${exchanges(chained)}",
               exchanges(chained) > exchanges(coPartitioned))

    assertEquals(chained.columns.sorted.toSeq, coPartitioned.columns.sorted.toSeq)
    val columns = chained.columns.sorted.map(col)
    assertEquals(sortedRows(chained.select(columns: _*)), sortedRows(coPartitioned.select(columns: _*)))
    assertEquals(4, coPartitioned.count())
  }
}
