package ai.chronon.spark.catalog

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CreationUtilsTest extends AnyFlatSpec with Matchers {

  "createTableSql" should "avoid Spark-reserved table properties" in {
    val sql = CreationUtils.createTableSql(
      tableName = "default.test_table",
      schema = StructType(Seq(StructField("value", StringType))),
      partitionColumns = List.empty,
      tableProperties = Map(
        "custom_property" -> "custom_value",
        "table_type" -> "iceberg",
        "TABLE_TYPE" -> "iceberg"
      ),
      tableTypeString = "iceberg"
    )

    sql should include("'custom_property'='custom_value'")
    sql should include("'file_format'='PARQUET'")
    sql should not include "table_type"
    sql should not include "TABLE_TYPE"
  }
}
