package ai.chronon.integrations.aws

import ai.chronon.api.{BooleanType, ByteType, DoubleType, FloatType, IntType, LongType, StringType, StructField, StructType}
import ai.chronon.online.CatalystUtil
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec

class CatalystUtilS3UDFTest extends AnyFlatSpec {

  private val inputSchema = StructType(
    "TestSchema",
    Array(StructField("int_val", IntType), StructField("str_val", StringType))
  )
  private val inputRow = Map[String, Any]("int_val" -> 10, "str_val" -> "hello")

  // Requires AWS credentials. Run manually after un-ignoring.
  it should "load a Hive UDF jar from S3 via ADD JAR and register it via CREATE FUNCTION" ignore {
    val sc = CatalystUtil.session.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider",
      "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")

    val setups = Seq(
      "ADD JAR 's3a://zipline-artifacts-canary/hive-udfs/chronon-test-udfs.jar'",
      "CREATE TEMPORARY FUNCTION MINUS_ONE AS 'ai.chronon.online.test.Minus_One'"
    )
    val cu = new CatalystUtil(inputSchema, selects = Seq("result" -> "MINUS_ONE(int_val)"), setups = setups)
    val resList = cu.performSql(inputRow)
    assertEquals(1, resList.size)
    assertEquals(9, resList.head("result"))
  }
}
