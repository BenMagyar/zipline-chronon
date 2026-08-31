package ai.chronon.integrations.aws

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
