package ai.chronon.spark.catalog

import org.apache.spark.sql.types.StructType

object CreationUtils {

  private val ALLOWED_TABLE_TYPES = List("iceberg", "delta", "hive", "parquet", "hudi")

  // Spark 4 reserves table_type. Iceberg catalogs populate the equivalent metastore marker themselves.
  private[catalog] def withoutTableTypeProperty(tableProperties: Map[String, String]): Map[String, String] =
    Option(tableProperties)
      .getOrElse(Map.empty)
      .filterNot { case (key, _) => key.equalsIgnoreCase("table_type") }

  /** Escapes a string value for use in SQL by doubling single quotes.
    * This follows the SQL standard for escaping string literals.
    *
    * @param value The string value to escape
    * @return The escaped string safe for SQL string literals
    */
  def escapeSqlStringValue(value: String): String = {
    value.replace("'", "''")
  }

  def createTableSql(tableName: String,
                     schema: StructType,
                     partitionColumns: List[String],
                     tableProperties: Map[String, String],
                     tableTypeString: String): String = {

    require(
      tableTypeString.isEmpty || ALLOWED_TABLE_TYPES.contains(tableTypeString.toLowerCase),
      s"Invalid table type: ${tableTypeString}. Must be empty OR one of: ${ALLOWED_TABLE_TYPES}"
    )

    val noPartitions = StructType(
      schema
        .filterNot(field => partitionColumns.contains(field.name)))

    val createFragment =
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         |    ${noPartitions.toDDL}
         |)
         |${if (tableTypeString.isEmpty) "" else f"USING ${tableTypeString}"}
         |""".stripMargin

    val partitionFragment = if (partitionColumns != null && partitionColumns.nonEmpty) {

      val partitionDefinitions = schema
        .filter(field => partitionColumns.contains(field.name))
        .map(field => s"${field.name} ${field.dataType.catalogString}")

      s"""PARTITIONED BY (
         |    ${partitionDefinitions.mkString(",\n    ")}
         |)""".stripMargin

    } else {
      ""
    }

    val settableTableProperties = withoutTableTypeProperty(tableProperties)
    val propertiesFragment = if (settableTableProperties.nonEmpty) {
      s"""TBLPROPERTIES (
         |    ${(settableTableProperties + ("file_format" -> "PARQUET"))
          .transform((k, v) => s"'${escapeSqlStringValue(k)}'='${escapeSqlStringValue(v)}'")
          .values
          .mkString(",\n   ")}
         |)""".stripMargin
    } else {
      ""
    }

    Seq(createFragment, partitionFragment, propertiesFragment).mkString("\n")

  }

  // Needs provider
  def alterTablePropertiesSql(tableName: String, properties: Map[String, String]): String = {
    // Only SQL api exists for setting TBLPROPERTIES
    val propertiesString = properties
      .map { case (key, value) =>
        s"'${escapeSqlStringValue(key)}' = '${escapeSqlStringValue(value)}'"
      }
      .mkString(", ")
    s"ALTER TABLE $tableName SET TBLPROPERTIES ($propertiesString)"
  }

}
