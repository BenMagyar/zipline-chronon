package ai.chronon.integrations.redis

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.ByteArrayOutputStream
import java.security.MessageDigest

object HadoopConditionalObjectWriter extends ConditionalObjectWriter {
  private val lock = new Object

  override def read(path: Path, hadoopConf: Configuration): Option[VersionedBytes] = lock.synchronized {
    val fs = path.getFileSystem(hadoopConf)
    if (fs.exists(path)) Some(versioned(path, readBytes(path, hadoopConf), hadoopConf)) else None
  }

  override def putIfAbsent(path: Path, contents: Array[Byte], hadoopConf: Configuration): VersionedBytes =
    lock.synchronized {
      read(path, hadoopConf).getOrElse {
        write(path, contents, overwrite = false, hadoopConf)
        versioned(path, contents, hadoopConf)
      }
    }

  override def compareAndSet(path: Path,
                             expectedVersion: String,
                             contents: Array[Byte],
                             hadoopConf: Configuration): VersionedBytes = lock.synchronized {
    val current =
      read(path, hadoopConf).getOrElse(throw new IllegalStateException(s"Conditional object does not exist: $path"))
    if (current.version != expectedVersion) current
    else {
      write(path, contents, overwrite = true, hadoopConf)
      versioned(path, contents, hadoopConf)
    }
  }

  private def write(path: Path, contents: Array[Byte], overwrite: Boolean, hadoopConf: Configuration): Unit = {
    val fs = path.getFileSystem(hadoopConf)
    Option(path.getParent).foreach(fs.mkdirs)
    val out = fs.create(path, overwrite)
    try {
      out.write(contents)
      out.hflush()
    } finally {
      out.close()
    }
  }

  private def readBytes(path: Path, hadoopConf: Configuration): Array[Byte] = {
    val in = path.getFileSystem(hadoopConf).open(path)
    val out = new ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    try {
      var count = in.read(buffer)
      while (count >= 0) {
        if (count > 0) out.write(buffer, 0, count)
        count = in.read(buffer)
      }
      out.toByteArray
    } finally {
      in.close()
      out.close()
    }
  }

  private def versioned(path: Path, contents: Array[Byte], hadoopConf: Configuration): VersionedBytes =
    VersionedBytes(
      contents,
      MessageDigest.getInstance("SHA-256").digest(contents).map(byte => f"${byte & 0xff}%02x").mkString,
      path.getFileSystem(hadoopConf).getFileStatus(path).getModificationTime
    )
}
