package ai.chronon.flink_connectors.kinesis

import ai.chronon.flink.source.FlinkSource
import ai.chronon.online.TopicInfo
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

/** Chronon Flink source that reads events from AWS Kinesis. Can be configured on the topic as:
  * kinesis://stream-name/tasks=20/<other-params>
  *
  * Config params such as the AWS region, access key ID, and secret access key are read from the
  * online properties (configured in teams.py or env variables passed via -Z flags).
  *
  * Kinesis differs from Kafka in a few aspects:
  * 1. Shard-based parallelism - we can derive parallelism based on the number of shards, but
  *    allow the user to override it via the 'tasks' property.
  * 2. Kinesis streams maintain their own checkpointing via shard iterators, so job restarts will
  *    resume from the last processed position.
  *
  * Required properties:
  * - AWS_REGION (or AWS_DEFAULT_REGION): The AWS region where the Kinesis stream exists
  * - AWS_ACCESS_KEY_ID: AWS access key for authentication
  * - AWS_SECRET_ACCESS_KEY: AWS secret access key for authentication
  *
  * Optional properties:
  * - tasks: Override the default parallelism
  * - KINESIS_ENDPOINT: Custom Kinesis endpoint (useful for local testing)
  * - initial_position: Starting position (LATEST, TRIM_HORIZON, or AT_TIMESTAMP)
  */
class KinesisFlinkSource[T](props: Map[String, String],
                            deserializationSchema: DeserializationSchema[T],
                            topicInfo: TopicInfo)
    extends FlinkSource[T] {

  // we use a small scale factor as streams are often over-provisioned. We can make this configurable via topicInfo
  val scaleFactor = 0.25

  private lazy val config = KinesisConfig.buildConsumerConfig(props, topicInfo)

  implicit lazy val parallelism: Int = config.explicitParallelism.getOrElse {
    math.ceil(KinesisConfig.getOpenShardCount(topicInfo.name, config.properties) * scaleFactor).toInt
  }

  override def getDataStream(topic: String, groupByName: String)(env: StreamExecutionEnvironment,
                                                                 parallelism: Int): SingleOutputStreamOperator[T] = {

    // The wrapper emits Array[T] because Chronon schemas can produce multiple records per message
    // (e.g. before+after rows, or multiple rows via explode). Additionally, the Kinesis deser schema only
    // allows for returning a single type, so we wrap the user-provided schema to emit Array[T].
    // We flatMap the array back into individual records immediately after the source.
    val wrappedSchema = new KinesisDeserializationSchemaWrapper[T](deserializationSchema)

    val kinesisConsumer = new FlinkKinesisConsumer[Array[T]](
      topicInfo.name,
      wrappedSchema,
      config.properties
    )

    // skip watermarks at the source as we derive them post Spark expr eval
    val noWatermarks: WatermarkStrategy[T] = WatermarkStrategy.noWatermarks()

    env
      .addSource(kinesisConsumer, s"Kinesis source: $groupByName - ${topicInfo.name}")
      .setParallelism(parallelism)
      .uid(s"kinesis-source-$groupByName")
      .flatMap[T](new FlatMapFunction[Array[T], T] {
                    override def flatMap(arr: Array[T], out: Collector[T]): Unit = arr.foreach(out.collect)
                  },
                  deserializationSchema.getProducedType)
      .uid(s"kinesis-source-flatmap-$groupByName")
      .assignTimestampsAndWatermarks(noWatermarks)
  }
}
