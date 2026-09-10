package ai.chronon.integrations.redis

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

case class VersionedBytes(bytes: Array[Byte], version: String, lastModifiedMillis: Long) {
  require(bytes != null, "Conditional object contents must be non-null")
  require(version != null && version.nonEmpty, "Conditional object version must be non-empty")
  require(lastModifiedMillis > 0, "Conditional object last-modified time must be positive")
}

/** Reads and conditionally replaces small coordination objects.
  *
  * AWS uses S3 ETags with conditional PUT. Local test implementations may serialize operations only within one JVM and
  * must not claim distributed compare-and-set support.
  */
trait ConditionalObjectWriter extends Serializable {
  def supportsDistributedCas: Boolean = false
  def read(path: Path, hadoopConf: Configuration): Option[VersionedBytes]
  def putIfAbsent(path: Path, contents: Array[Byte], hadoopConf: Configuration): VersionedBytes
  def compareAndSet(path: Path,
                    expectedVersion: String,
                    contents: Array[Byte],
                    hadoopConf: Configuration): VersionedBytes
}
