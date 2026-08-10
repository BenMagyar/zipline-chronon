package ai.chronon.spark.submission

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.JavaSerializer
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.source.SerializableTableWithSize
import org.apache.iceberg.types.Types
import org.apache.iceberg.{PartitionSpec, Schema, Table}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/** Regression spec for the driver-side StackOverflowError a Salesforce GroupBy backfill hit on
  * zipline engine 1.19.0 (EMR Serverless) when its snapshotEntities/ENTITIES path scanned an
  * Iceberg source table.
  *
  * Mechanism: Iceberg's `SparkBatch.planInputPartitions()` (and `SparkWrite` on the write side)
  * run `sparkContext.broadcast(SerializableTableWithSize.copyOf(table))` on the DRIVER — the
  * "broadcast_0" in the customer logs. Broadcast creation serializes the payload with
  * `spark.serializer`, which SparkSessionBuilder forces to Kryo with
  * `spark.kryo.referenceTracking=false`. Kryo's default FieldSerializer then has no cycle
  * detection, so a cycle in the payload's object graph (cloud catalog / FileIO / client-factory
  * back-references pulled in by the wrapped table) recurses until the driver thread dies with
  * StackOverflowError (ObjectField.write -> FieldSerializer.write -> Kryo.writeObject repeating).
  *
  * Fix under test: ChrononKryoRegistrator registers Iceberg's broadcast payload classes with
  * JavaSerializer (same pattern as the pre-existing GCSFileIO entry). Java serialization handles
  * cycles via handles, and Iceberg already Java-serializes these payloads into tasks, so they are
  * Java-safe by design.
  *
  * What this spec proves: the Kryo instance built from the production conf resolves JavaSerializer
  * for the broadcast payload classes, and that serializer round-trips a cyclic graph that kills
  * the default FieldSerializer. What it does NOT prove: that Salesforce's exact Glue/S3 object
  * graph was cyclic in a specific node — that payload shape is environment-specific and is modeled
  * here with a stand-in (see FakeCatalogIO below).
  */
class IcebergBroadcastKryoSpec extends AnyFlatSpec with Matchers {

  /** The Iceberg classes that land in driver-side `sparkContext.broadcast(...)` payloads per
    * scan/write (Iceberg 1.10.0, bundled via iceberg-spark-runtime-3.5). The metadata-table
    * variant backs scans of metadata tables (e.g. `table.partitions`).
    */
  private val broadcastPayloadClassNames = Seq(
    "org.apache.iceberg.spark.source.SerializableTableWithSize",
    "org.apache.iceberg.spark.source.SerializableTableWithSize$SerializableMetadataTableWithSize",
    "org.apache.iceberg.SerializableTable"
  )

  /** A Kryo instance built exactly the way the production driver builds it:
    * Spark's KryoSerializer + ChrononKryoRegistrator + spark.kryo.referenceTracking=false
    * (the setting SparkSessionBuilder hard-codes for batch jobs).
    */
  private def productionKryo: com.esotericsoftware.kryo.Kryo = {
    val conf = new SparkConf(loadDefaults = false)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "ai.chronon.spark.submission.ChrononKryoRegistrator")
      .set("spark.kryo.referenceTracking", "false")
    new KryoSerializer(conf).newKryo()
  }

  behavior of "ChrononKryoRegistrator Iceberg broadcast payload registration"

  it should "resolve JavaSerializer for Iceberg's scan/write broadcast payload classes" in {
    val kryo = productionKryo
    broadcastPayloadClassNames.foreach { name =>
      val cls = Class.forName(name)
      withClue(s"$name: ") {
        kryo.getRegistration(cls).getSerializer shouldBe a[JavaSerializer]
      }
    }
  }

  it should "serialize a cyclic graph through the serializer resolved for the broadcast payload class" in {
    val writeKryo = productionKryo
    val writeSerializer = writeKryo
      .getRegistration(Class.forName("org.apache.iceberg.spark.source.SerializableTableWithSize"))
      .getSerializer
      .asInstanceOf[com.esotericsoftware.kryo.Serializer[AnyRef]]
    // fail fast with a clear assertion (instead of an SOE) when the registration is missing
    writeSerializer shouldBe a[JavaSerializer]

    // a cyclic serializable graph, as reachable through catalog/FileIO back-references
    val cyclic = new FakeCatalogIO // cycle: FakeCatalogIO -> fileIO -> FakeCatalogIO
    val output = new Output(4096, -1)
    writeSerializer.write(writeKryo, output, cyclic)

    // read back with a fresh Kryo, mirroring production: the driver writes the broadcast and
    // executors read it with their own Kryo instances (a single JavaSerializer instance caches
    // its stream and cannot be reused for both directions)
    val readKryo = productionKryo
    val readSerializer = readKryo
      .getRegistration(Class.forName("org.apache.iceberg.spark.source.SerializableTableWithSize"))
      .getSerializer
      .asInstanceOf[com.esotericsoftware.kryo.Serializer[AnyRef]]
    val back = readSerializer
      .read(readKryo, new Input(output.toBytes), classOf[AnyRef])
      .asInstanceOf[FakeCatalogIO]
    back.fileIO.name shouldBe "s3-file-io"
    back.fileIO.catalog shouldBe theSameInstanceAs(back) // cycle preserved, no SOE
  }

  it should "still stackoverflow on unregistered cyclic graphs (documents why the registration matters)" in {
    val kryo = productionKryo
    // FakeCatalogIO is NOT registered, so Kryo falls back to the default FieldSerializer; with
    // referenceTracking=false the cycle recurses until StackOverflowError. intercept catches it
    // before it can take down the runner thread.
    intercept[StackOverflowError] {
      val output = new Output(4096, -1)
      kryo.writeClassAndObject(output, new FakeCatalogIO)
    }
  }

  it should "serialize the real Iceberg table broadcast payload for a plain hadoop-catalog table" in {
    // the exact object Iceberg broadcasts per scan: SerializableTableWithSize.copyOf(table)
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val tables = new HadoopTables(hadoopConf)
    val schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()))
    val tableDir = Files.createTempDirectory("iceberg-broadcast-kryo-spec").toString
    val table: Table = tables.create(schema, PartitionSpec.unpartitioned(), tableDir)
    val broadcastPayload: Table = SerializableTableWithSize.copyOf(table)

    val output = new Output(4096, -1)
    productionKryo.writeClassAndObject(output, broadcastPayload)
    output.position() should be > 0
  }
}

/** Stand-in for the cyclic object graphs that cloud-catalog Iceberg deployments drag into the
  * broadcast payload (e.g. a FileIO whose client-supplier lambda captures a factory/catalog
  * object that itself references the FileIO). Declared top-level so Kryo only sees the cycle,
  * not an enclosing-spec $outer reference.
  */
class FakeCatalogIO extends Serializable {
  val properties: Map[String, String] = Map("warehouse" -> "s3://bucket/warehouse")
  val fileIO: FakeFileIO = new FakeFileIO(this)
}
class FakeFileIO(val catalog: FakeCatalogIO) extends Serializable {
  val name: String = "s3-file-io"
}
