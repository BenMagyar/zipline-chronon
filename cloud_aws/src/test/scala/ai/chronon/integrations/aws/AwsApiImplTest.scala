package ai.chronon.integrations.aws

import ai.chronon.integrations.redis.RedisKVStoreFactory
import ai.chronon.online.fetcher.FetcherMain
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse, PutObjectRequest, S3Exception}

class AwsApiImplTest extends AnyFlatSpec with Matchers with MockitoSugar {
  "response logging" should "remain disabled while the AWS sink is unimplemented" in {
    val api = new AwsApiImpl(Map.empty) {
      def isResponseLoggingEnabled: Boolean = responseLoggingEnabled
    }

    api.isResponseLoggingEnabled shouldBe false
  }

  "configuredKvStoreType" should "default to DynamoDB" in {
    AwsApiImpl.configuredKvStoreType(Map.empty, Map.empty) shouldBe AwsApiImpl.DynamoDbKvStoreType
  }

  it should "select Redis from either API properties or the environment" in {
    AwsApiImpl.configuredKvStoreType(Map("kv.store.type" -> "ReDiS"), Map.empty) shouldBe
      AwsApiImpl.RedisKvStoreType
    AwsApiImpl.configuredKvStoreType(Map.empty, Map("KV_STORE_TYPE" -> "redis")) shouldBe
      AwsApiImpl.RedisKvStoreType
  }

  it should "let the runtime environment override the scaffold default" in {
    AwsApiImpl.configuredKvStoreType(
      Map("kv.store.type" -> "dynamodb"),
      Map("KV_STORE_TYPE" -> "redis")) shouldBe AwsApiImpl.RedisKvStoreType
  }

  it should "reject an unsupported implementation before creating a client" in {
    val exception = intercept[IllegalArgumentException] {
      new AwsApiImpl(Map("kv.store.type" -> "unknown")).genKvStore
    }

    exception.getMessage should include("Supported types: dynamodb, redis")
  }

  "FetcherMain" should "carry Redis selector and client properties through -Z" in {
    val parsed = new FetcherMain.Args(
      Array(
        "fetch",
        "--online-jar=/tmp/cloud_aws.jar",
        "--online-class=ai.chronon.integrations.aws.AwsApiImpl",
        "-Zkv.store.type=redis",
        "-Zredis.cluster.nodes=redis.example:6379",
        "-ZREDIS_USE_SSL=true"
      ))

    val properties = parsed.FetcherMainArgs.serializableProps
    properties shouldBe Map(
      "kv.store.type" -> "redis",
      "redis.cluster.nodes" -> "redis.example:6379",
      "REDIS_USE_SSL" -> "true"
    )
    AwsApiImpl.configuredKvStoreType(properties, Map.empty) shouldBe AwsApiImpl.RedisKvStoreType
    val settings = RedisKVStoreFactory.settings(properties, env = Map.empty)
    settings.nodes shouldBe "redis.example:6379"
    settings.ssl shouldBe true
  }

  "S3ConditionalObjectWriter" should "parse S3A control-record paths without changing the key" in {
    S3ConditionalObjectWriter.bucketAndKey(
      new Path("s3a://chronon-state/redis/source/dataset/_HEAD")) shouldBe
      ("chronon-state", "redis/source/dataset/_HEAD")
  }

  it should "reject non-S3 state paths" in {
    an[IllegalArgumentException] should be thrownBy {
      S3ConditionalObjectWriter.bucketAndKey(new Path("file:///tmp/redis-state/_HEAD"))
    }
  }

  it should "propagate definitive S3 write failures without retrying them as contention" in {
    val client = mock[S3Client]
    val forbidden = S3Exception.builder().statusCode(403).message("denied").build()
    when(client.putObject(any[PutObjectRequest], any[RequestBody])).thenThrow(forbidden)
    val writer = new S3ConditionalObjectWriter(() => client)

    val thrown = the[S3Exception] thrownBy {
      writer.compareAndSet(
        new Path("s3://chronon-state/redis/_HEAD"),
        "old-etag",
        "candidate".getBytes,
        new Configuration(false))
    }

    thrown shouldBe forbidden
    verify(client, never()).getObjectAsBytes(any[GetObjectRequest])
  }

  it should "rethrow an ambiguous CAS failure when S3 still contains the expected head" in {
    val client = mock[S3Client]
    val failure = SdkClientException.builder().message("connection reset").build()
    val oldBytes = "old-head".getBytes
    when(client.putObject(any[PutObjectRequest], any[RequestBody])).thenThrow(failure)
    when(client.getObjectAsBytes(any[GetObjectRequest])).thenReturn(
      ResponseBytes.fromByteArray(GetObjectResponse.builder().eTag("old-etag").build(), oldBytes))
    val writer = new S3ConditionalObjectWriter(() => client)

    val thrown = the[SdkClientException] thrownBy {
      writer.compareAndSet(
        new Path("s3://chronon-state/redis/_HEAD"),
        "old-etag",
        "candidate".getBytes,
        new Configuration(false))
    }

    thrown shouldBe failure
  }

  it should "reconcile an S3 service error after a conditional write was committed" in {
    val client = mock[S3Client]
    val failure = S3Exception.builder().statusCode(503).message("slow down").build()
    val candidate = "new-head".getBytes
    when(client.putObject(any[PutObjectRequest], any[RequestBody])).thenThrow(failure)
    when(client.getObjectAsBytes(any[GetObjectRequest])).thenReturn(
      ResponseBytes.fromByteArray(GetObjectResponse.builder().eTag("new-etag").build(), candidate))
    val writer = new S3ConditionalObjectWriter(() => client)

    val reconciled = writer.compareAndSet(
      new Path("s3://chronon-state/redis/_HEAD"),
      "old-etag",
      candidate,
      new Configuration(false))

    reconciled.bytes shouldBe candidate
    reconciled.version shouldBe "new-etag"
  }
}
