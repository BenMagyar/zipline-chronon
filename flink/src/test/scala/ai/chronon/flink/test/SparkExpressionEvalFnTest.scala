package ai.chronon.flink.test

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Constants
import ai.chronon.api.ScalaJavaConversions.{IteratorOps, JListOps, JMapOps}
import ai.chronon.flink.{SparkExpressionEval, SparkExpressionEvalFn}
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec

case class TimestampEvent(id: String, event_timestamp: java.sql.Timestamp)
case class TimestampMutationEvent(id: String,
                                  event_timestamp: java.sql.Timestamp,
                                  mutation_timestamp: java.sql.Timestamp,
                                  is_before: Boolean)

class SparkExpressionEvalFnTest extends AnyFlatSpec {

  private def evaluate[T](event: T,
                          encoder: Encoder[T],
                          query: ai.chronon.api.Query,
                          name: String,
                          dataModel: ai.chronon.api.DataModel = ai.chronon.api.DataModel.EVENTS): Map[String, Any] = {
    val evaluator = new SparkExpressionEval(encoder, query, name, dataModel)
    evaluator.initialize(new UnregisteredMetricsGroup)
    val serializer = encoder.asInstanceOf[ExpressionEncoder[T]].createSerializer()
    evaluator.evaluateExpressions(event, serializer).head
  }

  it should "filter on a non-selected source field in bulk validation" in {
    val groupBy = FlinkTestUtils.makeGroupBy(
      Seq("id"),
      filters = Seq("created > 1699366993123")
    )
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)
    val schema = StructType(
      Seq(
        StructField("id", StringType),
        StructField("int_val", IntegerType),
        StructField("double_val", DoubleType),
        StructField("created", LongType)
      ))
    val evaluator = new SparkExpressionEval(Encoders.row(schema), query, groupBy.metaData.name)
    evaluator.initialize(new UnregisteredMetricsGroup)

    val result = evaluator.runSparkSQLBulk(
      Seq(
        "test1" -> Row("test1", 12, 1.5, 1699366993123L),
        "test2" -> Row("test2", 13, 1.6, 1699366993124L)
      ))

    assert(result("test1").isEmpty)
    assert(result("test2").map(_("id")) == Seq("test2"))
    assert(result("test2").head.keySet == Set("id", "int_val", "double_val", Constants.TimeColumn))
  }

  it should "perform basic spark expr eval checks for event based groupbys" in {

    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, 1699366993123L),
      E2ETestEvent("test2", 13, 1.6, 1699366993124L),
      E2ETestEvent("test3", 14, 1.7, 1699366993125L)
    )

    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)
    val encoder = Encoders.product[E2ETestEvent]

    val sparkExprEval = new SparkExpressionEvalFn[E2ETestEvent](
      encoder,
      query,
      groupBy.metaData.name
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[E2ETestEvent] = env.fromCollection(elements.toJava)
    val sparkExprEvalDS = source.flatMap(sparkExprEval)

    val result = sparkExprEvalDS.executeAndCollect().toScala.toSeq
    // let's check the size
    assert(result.size == elements.size, "Expect result sets to include all 3 rows")
    // let's check the id field
    assert(result.map(_.apply("id")).toSet == Set("test1", "test2", "test3"))
  }

  it should "build output schema when time column is projected to ts" in {
    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)
    val outputSchema = new SparkExpressionEval(
      Encoders.product[E2ETestEvent],
      query,
      groupBy.metaData.name
    ).getOutputSchema

    assert(outputSchema(Constants.TimeColumn).dataType == LongType)
  }

  it should "convert timestamp time columns to millis" in {
    val expectedMillis = 1784291696789L
    val event = TimestampEvent("test", java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(expectedMillis)))
    val encoder = Encoders.product[TimestampEvent]
    val query = ai.chronon.api.Builders.Query(
      selects = Map(
        "id" -> "id"
      ),
      wheres = Seq(s"${Constants.TimeColumn} > 0"),
      timeColumn = "event_timestamp"
    )
    val result = evaluate(event, encoder, query, "timestamp_event")

    assert(result(Constants.TimeColumn) == expectedMillis)
  }

  it should "convert aliased timestamp time columns to millis" in {
    val expectedMillis = 1784291696789L
    val event = TimestampEvent("test", java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(expectedMillis)))
    val encoder = Encoders.product[TimestampEvent]
    val query = ai.chronon.api.Builders.Query(
      selects = Map(
        "id" -> "id",
        "event_time" -> "event_timestamp"
      ),
      timeColumn = "event_time"
    )
    val result = evaluate(event, encoder, query, "timestamp_alias_event")

    assert(result(Constants.TimeColumn) == expectedMillis)
  }

  it should "retain an authored timestamp projection when time column is implicit" in {
    val expectedMillis = 1784291696789L
    val event = TimestampEvent("test", java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(expectedMillis)))
    val encoder = Encoders.product[TimestampEvent]
    val query = ai.chronon.api.Builders.Query(
      selects = Map(
        "id" -> "id",
        Constants.TimeColumn -> "unix_millis(event_timestamp) + 7"
      )
    )
    val result = evaluate(event, encoder, query, "implicit_timestamp_event")

    assert(result(Constants.TimeColumn) == expectedMillis + 7)
  }

  it should "convert entity event and mutation timestamps to millis" in {
    val eventMillis = 1784291696789L
    val mutationMillis = eventMillis + 1000
    val event = TimestampMutationEvent(
      "test",
      java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(eventMillis)),
      java.sql.Timestamp.from(java.time.Instant.ofEpochMilli(mutationMillis)),
      is_before = false
    )
    val encoder = Encoders.product[TimestampMutationEvent]
    val query = ai.chronon.api.Builders.Query(
      selects = Map("id" -> "id"),
      timeColumn = "event_timestamp",
      mutationTimeColumn = "mutation_timestamp",
      reversalColumn = "is_before"
    )
    val result = evaluate(event, encoder, query, "timestamp_mutation_event", ai.chronon.api.DataModel.ENTITIES)

    assert(result(Constants.TimeColumn) == eventMillis)
    assert(result(Constants.MutationTimeColumn) == mutationMillis)
  }

  it should "allow groupbys to have null filters" in {

    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, 1699366993123L),
      E2ETestEvent("test2", 13, 1.6, 1699366993124L),
      E2ETestEvent("test3", 14, 1.7, 1699366993125L)
    )

    val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"), filters = null)
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)

    val encoder = Encoders.product[E2ETestEvent]

    val sparkExprEval = new SparkExpressionEvalFn[E2ETestEvent](
      encoder,
      query,
      groupBy.metaData.name
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[E2ETestEvent] = env.fromCollection(elements.toJava)
    val sparkExprEvalDS = source.flatMap(sparkExprEval)

    val result = sparkExprEvalDS.executeAndCollect().toScala.toSeq
    assert(result.size == elements.size, "Expect result sets to include all 3 rows")
  }

  it should "filter on a non-selected source field after projection" in {

    val elements = Seq(
      E2ETestEvent("test1", 12, 1.5, 1699366993123L),
      E2ETestEvent("test2", 13, 1.6, 1699366993124L),
      E2ETestEvent("test3", 14, 1.7, 1699366993125L)
    )

    val groupBy = FlinkTestUtils.makeGroupBy(
      Seq("id"),
      filters = Seq("created > 1699366993123")
    )
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)

    val encoder = Encoders.product[E2ETestEvent]

    val sparkExprEval = new SparkExpressionEvalFn[E2ETestEvent](
      encoder,
      query,
      groupBy.metaData.name
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[E2ETestEvent] = env.fromCollection(elements.toJava)
    val sparkExprEvalDS = source.flatMap(sparkExprEval)

    val result = sparkExprEvalDS.executeAndCollect().toScala.toSeq
    assert(result.map(_("id")).toSet == Set("test2", "test3"))
    assert(!result.exists(_.contains("created")))
  }

  it should "perform basic spark expr eval checks for entity based groupbys" in {

    val elements = Seq(
      E2ETestMutationEvent("test1", 12, 1.5, 1699366993123L, 1699366993123L, isBefore = true),
      E2ETestMutationEvent("test2", 13, 1.6, 1699366993124L, 1699366993124L, isBefore = false),
      E2ETestMutationEvent("test3", 14, 1.7, 1699366993125L, 1699366993125L, isBefore = true)
    )

    val groupBy = FlinkTestUtils.makeEntityGroupBy(Seq("id"))
    val query = SparkExpressionEval.queryFromGroupBy(groupBy)
    val encoder = Encoders.product[E2ETestMutationEvent]

    val sparkExprEval = new SparkExpressionEvalFn[E2ETestMutationEvent](
      encoder,
      query,
      groupBy.metaData.name,
      groupBy.dataModel
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[E2ETestMutationEvent] = env.fromCollection(elements.toJava)
    val sparkExprEvalDS = source.flatMap(sparkExprEval)

    val result = sparkExprEvalDS.executeAndCollect().toScala.toSeq
    // let's check the size
    assert(result.size == elements.size, "Expect result sets to include all 3 rows")
    // let's check the id field
    assert(result.map(_.apply("id")).toSet == Set("test1", "test2", "test3"))
    // let's check the isBefore field
    assert(result.map(_.apply("is_before")).toSet == Set(true, false))
  }
}
