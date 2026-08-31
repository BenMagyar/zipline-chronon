package ai.chronon.integrations.aws

import ai.chronon.integrations.redis.{ConditionalObjectWriter, VersionedBytes}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, PutObjectRequest, S3Exception}

import java.util.{Arrays, Locale}
import scala.util.control.NonFatal

/** S3-backed create-if-absent used to serialize Redis batch generations per dataset.
  *
  * The `If-None-Match: *` condition is the durability boundary. A lost response is reconciled by reading the object
  * that won; the caller verifies that an existing immutable record has the expected bytes.
  */
private[aws] class S3ConditionalObjectWriter(clientFactory: () => S3Client) extends ConditionalObjectWriter {
  override val supportsDistributedCas: Boolean = true

  override def read(path: Path, hadoopConf: Configuration): Option[VersionedBytes] = {
    val (bucket, key) = bucketAndKey(path)
    val client = clientFactory()
    try read(client, bucket, key)
    finally client.close()
  }

  override def putIfAbsent(path: Path, contents: Array[Byte], hadoopConf: Configuration): VersionedBytes = {
    val (bucket, key) = bucketAndKey(path)
    val client = clientFactory()
    try {
      val request = PutObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .ifNoneMatch("*")
        .build()
      try {
        client.putObject(request, RequestBody.fromBytes(contents))
        read(client, bucket, key).getOrElse(
          throw new IllegalStateException(s"S3 conditional object disappeared after PUT: s3://$bucket/$key"))
      } catch {
        case error: S3Exception if shouldReconcile(error) =>
          reconcile(client, bucket, key, contents, expectedVersion = None, error)
        case error: S3Exception => throw error
        case NonFatal(error)    => reconcile(client, bucket, key, contents, expectedVersion = None, error)
      }
    } finally {
      client.close()
    }
  }

  override def compareAndSet(path: Path,
                             expectedVersion: String,
                             contents: Array[Byte],
                             hadoopConf: Configuration): VersionedBytes = {
    val (bucket, key) = bucketAndKey(path)
    val client = clientFactory()
    try {
      val request = PutObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .ifMatch(expectedVersion)
        .build()
      try {
        client.putObject(request, RequestBody.fromBytes(contents))
        read(client, bucket, key).getOrElse(
          throw new IllegalStateException(s"S3 conditional object disappeared after PUT: s3://$bucket/$key"))
      } catch {
        case error: S3Exception if shouldReconcile(error) =>
          reconcile(client, bucket, key, contents, Some(expectedVersion), error)
        case error: S3Exception => throw error
        case NonFatal(error)    => reconcile(client, bucket, key, contents, Some(expectedVersion), error)
      }
    } finally {
      client.close()
    }
  }

  private[aws] def bucketAndKey(path: Path): (String, String) = {
    val uri = path.toUri
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT))
    require(scheme.exists(value => value == "s3" || value == "s3a" || value == "s3n"),
            s"Redis AWS state path must use an S3 scheme: $path")
    val bucket = Option(uri.getAuthority)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"Redis AWS state path has no S3 bucket: $path"))
    val key = Option(uri.getPath).getOrElse("").stripPrefix("/")
    require(key.nonEmpty, s"Redis AWS state path has no object key: $path")
    bucket -> key
  }

  private def read(client: S3Client, bucket: String, key: String): Option[VersionedBytes] =
    try {
      val response = client.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(key).build())
      val lastModifiedMillis = Option(response.response().lastModified())
        .map(_.toEpochMilli)
        .getOrElse(System.currentTimeMillis())
      Some(VersionedBytes(response.asByteArray(), response.response().eTag(), lastModifiedMillis))
    } catch {
      case error: S3Exception if error.statusCode() == 404 => None
    }

  private def shouldReconcile(error: S3Exception): Boolean =
    error.statusCode() == 409 || error.statusCode() == 412 || error.statusCode() >= 500

  private def reconcile(client: S3Client,
                        bucket: String,
                        key: String,
                        candidate: Array[Byte],
                        expectedVersion: Option[String],
                        originalError: Throwable): VersionedBytes = {
    val current =
      try read(client, bucket, key)
      catch {
        case NonFatal(_) => throw originalError
      }
    current match {
      case Some(value) if Arrays.equals(value.bytes, candidate)      => value
      case Some(value) if expectedVersion.forall(_ != value.version) => value
      case _                                                         => throw originalError
    }
  }
}

object S3ConditionalObjectWriter extends S3ConditionalObjectWriter(() => S3Client.builder().build())
