package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.{IntType, StringType, StructField, StructType}
import ai.chronon.online.CatalystUtil
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec

class CatalystUtilGCSUDFTest extends AnyFlatSpec {

  private val inputSchema = StructType(
    "TestSchema",
    Array(StructField("int_val", IntType), StructField("str_val", StringType))
  )
  private val inputRow = Map[String, Any]("int_val" -> 10, "str_val" -> "hello")

  // Requires GCS credentials (gcloud auth application-default login). Run manually after un-ignoring.
  it should "load a Hive UDF jar from GCS via ADD JAR and register it via CREATE FUNCTION" ignore {
    val sc = CatalystUtil.session.sparkContext
    sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    sc.hadoopConfiguration.set("fs.gs.auth.type", "APPLICATION_DEFAULT")

    val setups = Seq(
      "ADD JAR 'gs://zipline-artifacts-canary/hive-udfs/chronon-test-udfs.jar'",
      "CREATE TEMPORARY FUNCTION MINUS_ONE AS 'ai.chronon.online.test.Minus_One'"
    )
    val cu = new CatalystUtil(inputSchema, selects = Seq("result" -> "MINUS_ONE(int_val)"), setups = setups)
    val resList = cu.performSql(inputRow)
    assertEquals(1, resList.size)
    assertEquals(9, resList.head("result"))
  }
}
