package ai.chronon.spark.catalog

import ai.chronon.spark.utils.SparkTestBase
import org.junit.Assert.assertEquals

class FormatTest extends SparkTestBase {

  override def sparkConfs: Map[String, String] = Map(
    "spark.sql.catalog.spark_non_default_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.spark_non_default_catalog.type" -> "hadoop",
    "spark.sql.catalog.spark_non_default_catalog.warehouse" -> icebergWarehouse
  )

  it should "resolve table names consistently with Spark SQL" in {
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_non_default_catalog.custom_test_db")
    spark.sql(
      "CREATE TABLE spark_non_default_catalog.custom_test_db.resolve_test_table (id INT, ds STRING) USING iceberg PARTITIONED BY (ds)")
    try {
      val tableName = "spark_non_default_catalog.custom_test_db.resolve_test_table"
      val resolved = Format.resolveTableName(tableName)
      val sparkTable = spark.catalog.getTable(tableName)
      assertEquals(sparkTable.catalog, resolved.catalog)
      assertEquals(sparkTable.database, resolved.namespace)
      assertEquals(sparkTable.name, resolved.table)
    } finally {
      spark.sql("DROP TABLE IF EXISTS spark_non_default_catalog.custom_test_db.resolve_test_table")
      spark.sql("DROP DATABASE IF EXISTS spark_non_default_catalog.custom_test_db")
    }
  }

  it should "resolve fully qualified table names" in {
    assertEquals(ResolvedTableName("catalogA", "ns", "tbl"), Format.resolveTableName("catalogA.ns.tbl"))
    assertEquals(ResolvedTableName("catalogA", "ns", "tbl"), Format.resolveTableName("`catalogA`.ns.tbl"))
  }

  it should "resolve two-part table names with default catalog" in {
    assertEquals(ResolvedTableName("spark_catalog", "mydb", "tbl"), Format.resolveTableName("mydb.tbl"))
  }

  it should "resolve single-part table names with default catalog and current database" in {
    val resolved = Format.resolveTableName("tbl")
    assertEquals("spark_catalog", resolved.catalog)
    assertEquals(spark.catalog.currentDatabase, resolved.namespace)
    assertEquals("tbl", resolved.table)
  }

  it should "strip destination catalog in rename SQL when source and destination share a catalog" in {
    val sql = Format.renameTableSql(
      "spark_non_default_catalog.custom_test_db.src_table",
      "spark_non_default_catalog.custom_test_db.dest_table"
    )
    assertEquals(
      "ALTER TABLE spark_non_default_catalog.custom_test_db.src_table RENAME TO `custom_test_db`.`dest_table`",
      sql)
  }

  it should "keep destination catalog in rename SQL when source and destination catalogs differ" in {
    val sql = Format.renameTableSql(
      "spark_non_default_catalog.custom_test_db.src_table",
      "spark_catalog.default.dest_table"
    )
    assertEquals(
      "ALTER TABLE spark_non_default_catalog.custom_test_db.src_table RENAME TO `spark_catalog`.`default`.`dest_table`",
      sql)
  }

}
