package ai.chronon.flink.test.source

import ai.chronon.flink.source.{FlinkSourceProvider, KafkaFlinkSource}
import ai.chronon.online.TopicInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class KafkaFlinkSourceSpec extends AnyFlatSpec {

  it should "fail on missing kafka bootstrap servers" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)
    assertThrows[Exception] {
      FlinkSourceProvider.build(Map.empty, null, topicInfo)
    }
  }

  it should "read kafka bootstrap server from properties" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)
    val flinkSrc = FlinkSourceProvider.build(Map(KafkaFlinkSource.KafkaBootstrap -> "my-kafka-server"), null, topicInfo)

    flinkSrc.isInstanceOf[KafkaFlinkSource[_]] shouldBe true
    val kafkaSrc = flinkSrc.asInstanceOf[KafkaFlinkSource[_]]
    kafkaSrc.bootstrap shouldBe "my-kafka-server"
  }

  it should "read kafka bootstrap server from topic info" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map(KafkaFlinkSource.KafkaBootstrap -> "my-kafka-server"))
    val flinkSrc = FlinkSourceProvider.build(Map.empty, null, topicInfo)

    flinkSrc.isInstanceOf[KafkaFlinkSource[_]] shouldBe true
    val kafkaSrc = flinkSrc.asInstanceOf[KafkaFlinkSource[_]]
    kafkaSrc.bootstrap shouldBe "my-kafka-server"
  }

  it should "read kafka bootstrap server from host and port" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map("host" -> "my-host", "port" -> "9092"))
    val flinkSrc = FlinkSourceProvider.build(Map.empty, null, topicInfo)

    flinkSrc.isInstanceOf[KafkaFlinkSource[_]] shouldBe true
    val kafkaSrc = flinkSrc.asInstanceOf[KafkaFlinkSource[_]]
    kafkaSrc.bootstrap shouldBe "my-host:9092"
  }

  it should "override partition-based parallelism from topic info" in {
    val topicInfo = TopicInfo(
      "test-topic",
      "kafka",
      Map(KafkaFlinkSource.KafkaBootstrap -> "my-kafka-server", KafkaFlinkSource.TaskParallelism -> "5")
    )
    val flinkSrc = FlinkSourceProvider.build(Map.empty, null, topicInfo)

    flinkSrc.parallelism shouldBe 5
  }

  it should "prefer a parallelism override from properties over topic info" in {
    val props = Map(
      KafkaFlinkSource.KafkaBootstrap -> "my-kafka-server",
      KafkaFlinkSource.TaskParallelism -> "7"
    )
    val topicInfo = TopicInfo("test-topic", "kafka", Map(KafkaFlinkSource.TaskParallelism -> "5"))
    val flinkSrc = FlinkSourceProvider.build(props, null, topicInfo)

    flinkSrc.parallelism shouldBe 7
  }

  // --- resolveSaslJaasConfig ---

  "resolveSaslJaasConfig" should "inject sasl.jaas.config from env when security.protocol contains SASL and config is not set" in {
    val params = Map("security.protocol" -> "SASL_SSL", "bootstrap" -> "kafka:9093")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, {
      case KafkaFlinkSource.SaslJaasConfigEnvVar => Some("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";")
      case _ => None
    })
    resolved("sasl.jaas.config") shouldBe "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"pass\";"
  }

  it should "not overwrite sasl.jaas.config if already set in params" in {
    val existingJaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"existing\";"
    val params = Map("security.protocol" -> "SASL_SSL", "sasl.jaas.config" -> existingJaas)
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => Some("should-not-be-used"))
    resolved("sasl.jaas.config") shouldBe existingJaas
  }

  it should "leave params unchanged when security.protocol does not contain SASL" in {
    val params = Map("security.protocol" -> "SSL", "bootstrap" -> "kafka:9093")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => Some("should-not-be-used"))
    resolved should not contain key("sasl.jaas.config")
  }

  it should "leave params unchanged when security.protocol is not set" in {
    val params = Map("bootstrap" -> "kafka:9092")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => Some("should-not-be-used"))
    resolved should not contain key("sasl.jaas.config")
  }

  it should "leave params unchanged when SASL_JAAS_CONFIG env var is not set" in {
    val params = Map("security.protocol" -> "SASL_PLAINTEXT")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => None)
    resolved should not contain key("sasl.jaas.config")
  }

  it should "fetch sasl.jaas.config from SASL_JAAS_CONFIG_VAULT_URI when the plain env var is not set" in {
    val arn = "arn:aws:secretsmanager:us-west-2:123456789012:secret:kafka-jaas"
    val params = Map("security.protocol" -> "SASL_SSL")
    val env = Map("SASL_JAAS_CONFIG_VAULT_URI" -> arn)
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, env.get, uri => s"jaas-from-$uri")
    resolved("sasl.jaas.config") shouldBe s"jaas-from-$arn"
  }

  it should "prefer SASL_JAAS_CONFIG over the vault ref" in {
    val params = Map("security.protocol" -> "SASL_SSL")
    val env = Map("SASL_JAAS_CONFIG" -> "plain-jaas", "SASL_JAAS_CONFIG_VAULT_URI" -> "arn:unused")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, env.get, _ => fail("should not fetch"))
    resolved("sasl.jaas.config") shouldBe "plain-jaas"
  }

  it should "fail when the vault ref cannot be fetched" in {
    val params = Map("security.protocol" -> "SASL_SSL")
    val env = Map("SASL_JAAS_CONFIG_VAULT_URI" -> "arn:aws:secretsmanager:us-west-2:1:secret:missing")
    an[RuntimeException] should be thrownBy
      KafkaFlinkSource.resolveSaslJaasConfig(params, env.get, _ => throw new RuntimeException("AccessDenied"))
  }

  it should "inject sasl.jaas.config from env when existing value is empty" in {
    val params = Map("security.protocol" -> "SASL_SSL", "sasl.jaas.config" -> "")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => Some("injected-jaas"))
    resolved("sasl.jaas.config") shouldBe "injected-jaas"
  }

  it should "inject sasl.jaas.config from env when existing value is whitespace-only" in {
    val params = Map("security.protocol" -> "SASL_SSL", "sasl.jaas.config" -> "   ")
    val resolved = KafkaFlinkSource.resolveSaslJaasConfig(params, _ => Some("injected-jaas"))
    resolved("sasl.jaas.config") shouldBe "injected-jaas"
  }
}
