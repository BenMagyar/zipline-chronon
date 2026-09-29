package ai.chronon.flink.source

import ai.chronon.api.secrets.VaultSecretProvider
import ai.chronon.flink.FlinkUtils
import ai.chronon.online.{TopicChecker, TopicInfo}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.slf4j.LoggerFactory

class KafkaFlinkSource[T](props: Map[String, String],
                          deserializationSchema: DeserializationSchema[T],
                          topicInfo: TopicInfo,
                          getEnv: String => Option[String] = key => Option(System.getenv(key)))
    extends FlinkSource[T] {
  import KafkaFlinkSource._

  // Resolve Kafka params once: if security.protocol contains SASL and sasl.jaas.config is not
  // already set, fall back to the SASL_JAAS_CONFIG env var or its SASL_JAAS_CONFIG_VAULT_URI ref
  // (FLINK_-prefixed in node config; the prefix is stripped when injected as a pod env var).
  val resolvedParams: Map[String, String] =
    resolveSaslJaasConfig(topicInfo.params, getEnv)

  val bootstrap: String = {
    // we first check props for the bootstrap server and fallback to topicInfo if not found
    FlinkUtils
      .getProperty(KafkaBootstrap, props, topicInfo)
      .orElse(getBootstrapFromHostPort(resolvedParams.get("host"), resolvedParams.get("port")))
      .getOrElse(throw new IllegalArgumentException("No bootstrap servers provided"))
  }

  // we use a small scale factor as topics are often over partitioned. We can make this configurable via topicInfo
  val scaleFactor = 0.25

  implicit lazy val parallelism: Int = {
    FlinkUtils
      .getProperty(TaskParallelism, props, topicInfo)
      .map(_.toInt)
      .getOrElse(math.ceil(TopicChecker.getPartitions(topicInfo.name, bootstrap, resolvedParams) * scaleFactor).toInt)
  }

  override def getDataStream(topic: String, groupByName: String)(env: StreamExecutionEnvironment,
                                                                 parallelism: Int): SingleOutputStreamOperator[T] = {
    // confirm the topic exists
    TopicChecker.topicShouldExist(topicInfo.name, bootstrap, resolvedParams)

    val startingOffsets = FlinkUtils.getProperty("start_offset", props, topicInfo) match {
      case Some(timestampStr) =>
        try {
          val timestamp = timestampStr.toLong
          OffsetsInitializer.timestamp(timestamp)
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Invalid timestamp format for start_offset: '$timestampStr'. Expected a valid long value in milliseconds.")
        }
      case None =>
        OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)
    }

    val kafkaSource = KafkaSource
      .builder[T]()
      .setTopics(topicInfo.name)
      .setGroupId(s"chronon-$groupByName")
      // starting offsets are configurable via start_offset (timestamp) or default to latest when starting afresh
      .setStartingOffsets(startingOffsets)
      .setValueOnlyDeserializer(deserializationSchema)
      .setBootstrapServers(bootstrap)
      .setProperties(TopicChecker.mapToJavaProperties(resolvedParams))
      .build()

    env
      .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), s"Kafka source: $groupByName - ${topicInfo.name}")
      .setParallelism(parallelism)
  }
}

object KafkaFlinkSource {
  private val logger = LoggerFactory.getLogger(getClass)

  val KafkaBootstrap = "bootstrap"
  val TaskParallelism = "tasks"
  val SecurityProtocol = "security.protocol"
  val SaslJaasConfig = "sasl.jaas.config"
  val SaslJaasConfigEnvVar = "SASL_JAAS_CONFIG"
  val SaslJaasConfigVaultUriEnvVar = "SASL_JAAS_CONFIG_VAULT_URI"

  def getBootstrapFromHostPort(maybeHost: Option[String], maybePort: Option[String]): Option[String] = {
    maybeHost
      .map(host => host + maybePort.map(":" + _).getOrElse(""))
  }

  /** If security.protocol contains SASL and sasl.jaas.config is not already set, populates it from
    * SASL_JAAS_CONFIG or, failing that, fetches the secret SASL_JAAS_CONFIG_VAULT_URI points at (a
    * failed fetch throws). Resolving here rather than at submission keeps the plaintext out of the
    * FlinkDeployment spec. With neither set, params pass through: a JVM-wide JAAS file may apply.
    */
  def resolveSaslJaasConfig(
      params: Map[String, String],
      getEnv: String => Option[String],
      fetchSecret: String => String = uri => VaultSecretProvider.forUri(uri).getSecret(uri)): Map[String, String] = {
    val isSasl = params.get(SecurityProtocol).exists(_.toUpperCase.contains("SASL"))
    if (isSasl && !params.get(SaslJaasConfig).exists(_.trim.nonEmpty)) {
      getEnv(SaslJaasConfigEnvVar)
        .filter(_.trim.nonEmpty)
        .orElse(getEnv(SaslJaasConfigVaultUriEnvVar).filter(_.trim.nonEmpty).map(fetchSecret)) match {
        case Some(jaasConfig) => params + (SaslJaasConfig -> jaasConfig)
        case None =>
          logger.warn(
            s"$SecurityProtocol is SASL but neither $SaslJaasConfigEnvVar nor $SaslJaasConfigVaultUriEnvVar is set")
          params
      }
    } else {
      params
    }
  }
}
