package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.PartitionRange
import ai.chronon.spark.catalog.{FormatProvider, Iceberg, TableUtils}
import ai.chronon.spark.submission.{ChrononKryoRegistrator, SparkSessionBuilder}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.cloud.spark.bigquery.SparkBigQueryUtil
import org.apache.iceberg.gcp.bigquery.{BigQueryMetastoreCatalog => BQMSCatalog}
import org.apache.iceberg.gcp.gcs.GCSFileIO
import org.apache.iceberg.io.ResolvingFileIO
import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.internal.SQLConf
import org.junit.Assert.{assertEquals, assertNotNull, assertTrue}
import org.objenesis.strategy.StdInstantiatorStrategy
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.collection.JavaConverters._

class BigQueryCatalogTest extends AnyFlatSpec with MockitoSugar {

  lazy val spark: SparkSession = SparkSessionBuilder.build(
    "BigQuerySparkTest",
    local = true,
    additionalConfig = Some(
      Map(
        "spark.chronon.table.format_provider.class" -> classOf[GcpFormatProvider].getName,
        "spark.chronon.partition.column" -> "ds",
//        "spark.hadoop.fs.gs.impl" -> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
//        "spark.hadoop.fs.AbstractFileSystem.gs.impl" -> "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        "spark.sql.catalogImplementation" -> "in-memory",

//        Uncomment to test
//        "spark.sql.defaultCatalog" -> "default_iceberg",
//        "spark.sql.catalog.default_iceberg" -> classOf[SparkCatalog].getName,
//        "spark.sql.catalog.default_iceberg.catalog-impl" -> classOf[BQMSCatalog].getName,
//        "spark.sql.catalog.default_iceberg.io-impl" -> classOf[ResolvingFileIO].getName,
//        "spark.sql.catalog.default_iceberg.warehouse" -> "gs://zipline-warehouse-canary/data/tables/",
//        "spark.sql.catalog.default_iceberg.gcp.bigquery.location" -> "us-central1",
//        "spark.sql.catalog.default_iceberg.gcp.bigquery.project-id" -> "canary-443022",
//        "spark.kryo.registrator" -> classOf[ChrononKryoRegistrator].getName,
//        "spark.sql.defaultUrlStreamHandlerFactory.enabled" -> false.toString,
      ))
  )
  lazy val tableUtils: TableUtils = TableUtils(spark)

  it should "works with views" ignore {
    val viewName = "data.purchases_native_view"
    val nativeName = "data.purchases"

    val viewParts =
      tableUtils.partitions(viewName,
                            partitionRange =
                              Option(PartitionRange("2023-11-01", "2023-11-30")(tableUtils.partitionSpec)))
    assertEquals(30, viewParts.size)
    val nativeParts =
      tableUtils.partitions(nativeName,
                            partitionRange =
                              Option(PartitionRange("2023-11-01", "2023-11-30")(tableUtils.partitionSpec)))
    assertEquals(30, nativeParts.size)

    assertEquals(nativeParts.toSet, viewParts.toSet)

  }

  it should "works with a partition range for views and tables" ignore {
    val viewName = "data.purchases_native_view"
    val nativeName = "data.purchases"
    val viewTruncated =
      tableUtils.partitions(viewName,
                            partitionRange =
                              Option(PartitionRange("2023-11-28", "2023-11-30")(tableUtils.partitionSpec)))
    assertEquals(3, viewTruncated.size)
    val nativeTruncated =
      tableUtils.partitions(nativeName,
                            partitionRange =
                              Option(PartitionRange("2023-11-28", "2023-11-30")(tableUtils.partitionSpec)))
    assertEquals(3, nativeTruncated.size)

    assertEquals(nativeTruncated.toSet, viewTruncated.toSet)

  }

  it should "verify dynamic classloading of GCP providers" in {
    assertTrue(FormatProvider.from(spark).isInstanceOf[GcpFormatProvider])
  }

  it should "be consistent about parsing table names for spark and bigquery" ignore {
    val sparkTable = "`project-id`.dataset.table_name"

    val bTableId = SparkBQUtils.toTableId(sparkTable)(spark)

    assertEquals("table_name", bTableId.getTable)
    assertEquals("dataset", bTableId.getDataset)
    assertEquals("project-id", bTableId.getProject)

    val invalidSparkTableName = "project-id.dataset.table_name"
    assertThrows[ParseException] {
      val bTableId = SparkBQUtils.toTableId(invalidSparkTableName)(spark)
    }
  }

  it should "table renames in bigquery catalog" ignore {
    // Create an empty dataframe with a simple schema
    val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType, nullable = false),
        org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType, nullable = true)
      )))

    val originalTableName = "data.test_rename_original"
    val renamedTableName = "data.test_rename_new"

    // Clean up any existing tables
    try { spark.sql(s"DROP TABLE IF EXISTS $originalTableName") } catch { case _: Exception => }
    try { spark.sql(s"DROP TABLE IF EXISTS $renamedTableName") } catch { case _: Exception => }

    // Write the empty dataframe to create a table
    tableUtils.insertPartitions(emptyDf,
      originalTableName,
      Map("file_format" -> "PARQUET", "table_type" -> "iceberg"),
      List.empty)

    // Verify the original table exists
    assertTrue(s"Original table $originalTableName should exist", tableUtils.tableReachable(originalTableName))

    // Rename the table using ALTER TABLE
    spark.sql(s"ALTER TABLE $originalTableName RENAME TO $renamedTableName")

    // Verify the table was renamed
    assertTrue(s"Renamed table $renamedTableName should exist", tableUtils.tableReachable(renamedTableName))
    assertTrue(s"Original table $originalTableName should not exist", !tableUtils.tableReachable(originalTableName))

    // Clean up
    try { spark.sql(s"DROP TABLE IF EXISTS $renamedTableName") } catch { case _: Exception => }
  }

  it should "correctly parse table names to Spark Identifiers" in {
    // Test simple table name
    val simpleTable = "table_name"
    val simpleIdentifier = SparkBQUtils.toIdentifier(simpleTable)(spark)
    assertEquals("table_name", simpleIdentifier.name())
    assertEquals(0, simpleIdentifier.namespace().length)

    // Test database.table formatN
    val databaseTable = "database.table_name"
    val databaseIdentifier = SparkBQUtils.toIdentifier(databaseTable)(spark)
    assertEquals("table_name", databaseIdentifier.name())
    assertEquals(1, databaseIdentifier.namespace().length)
    assertEquals("database", databaseIdentifier.namespace()(0))

    // Test catalog.database.table format
    val catalogDatabaseTable = "catalog.database.table_name"
    val catalogIdentifier = SparkBQUtils.toIdentifier(catalogDatabaseTable)(spark)
    assertEquals("table_name", catalogIdentifier.name())
    assertEquals(2, catalogIdentifier.namespace().length)
    assertEquals("catalog", catalogIdentifier.namespace()(0))
    assertEquals("database", catalogIdentifier.namespace()(1))
  }

  it should "handle quoted identifiers correctly" in {
    // Test quoted project ID with hyphens
    val quotedProjectTable = "`project-id`.dataset.table_name"
    val quotedIdentifier = SparkBQUtils.toIdentifier(quotedProjectTable)(spark)
    assertEquals("table_name", quotedIdentifier.name())
    assertEquals(2, quotedIdentifier.namespace().length)
    assertEquals("project-id", quotedIdentifier.namespace()(0))
    assertEquals("dataset", quotedIdentifier.namespace()(1))

    // Test quoted table name with special characters
    val quotedTableName = "catalog.dataset.`table-with-hyphens`"
    val quotedTableIdentifier = SparkBQUtils.toIdentifier(quotedTableName)(spark)
    assertEquals("table-with-hyphens", quotedTableIdentifier.name())
    assertEquals(2, quotedTableIdentifier.namespace().length)
    assertEquals("catalog", quotedTableIdentifier.namespace()(0))
    assertEquals("dataset", quotedTableIdentifier.namespace()(1))
  }

  it should "handle complex BigQuery table names" in {
    // Test realistic BigQuery table name with project, dataset, and table
    val bigQueryTable = "`my-project-123`.my_dataset.my_table_name"
    val bigQueryIdentifier = SparkBQUtils.toIdentifier(bigQueryTable)(spark)
    assertEquals("my_table_name", bigQueryIdentifier.name())
    assertEquals(2, bigQueryIdentifier.namespace().length)
    assertEquals("my-project-123", bigQueryIdentifier.namespace()(0))
    assertEquals("my_dataset", bigQueryIdentifier.namespace()(1))

    // Test table name with underscores and numbers
    val complexTable = "project123.dataset_test.table_name_v2"
    val complexIdentifier = SparkBQUtils.toIdentifier(complexTable)(spark)
    assertEquals("table_name_v2", complexIdentifier.name())
    assertEquals(2, complexIdentifier.namespace().length)
    assertEquals("project123", complexIdentifier.namespace()(0))
    assertEquals("dataset_test", complexIdentifier.namespace()(1))
  }

  it should "bigquery connector converts spark dates regardless of date setting" in {
    val input = spark.createDataFrame(Seq((1, "2021-01-01"))).toDF("id", "ds")
    spark.conf.set(SQLConf.DATETIME_JAVA8API_ENABLED.key, true)
    val java8Date = input.select(col("id"), to_date(col("ds"))).collect.take(1).head.get(1)
    assert(java8Date.isInstanceOf[java.time.LocalDate])
    SparkBigQueryUtil.sparkDateToBigQuery(java8Date)

    spark.conf.set(SQLConf.DATETIME_JAVA8API_ENABLED.key, false)
    val nonJava8Date = input.select(col("id"), to_date(col("ds"))).collect.take(1).head.get(1)
    assert(nonJava8Date.isInstanceOf[java.sql.Date])
    SparkBigQueryUtil.sparkDateToBigQuery(nonJava8Date)
  }

  it should "bigquery connector converts spark timestamp regardless of setting" in {
    val input = spark.createDataFrame(Seq((1, "2025-04-28 12:30:45"))).toDF("id", "ts")
    spark.conf.set(SQLConf.DATETIME_JAVA8API_ENABLED.key, true)
    val java8Timestamp = input.select(col("id"), col("ts").cast("timestamp")).collect.take(1).head.get(1)
    assert(java8Timestamp.isInstanceOf[java.time.Instant])
    SparkBigQueryUtil.sparkTimestampToBigQuery(java8Timestamp)

    spark.conf.set(SQLConf.DATETIME_JAVA8API_ENABLED.key, false)
    val nonJava8Timestamp = input.select(col("id"), col("ts").cast("timestamp")).collect.take(1).head.get(1)
    assert(nonJava8Timestamp.isInstanceOf[java.sql.Timestamp])
    SparkBigQueryUtil.sparkTimestampToBigQuery(nonJava8Timestamp)
  }

  it should "integration testing bigquery native table" ignore {
    val nativeTable = "data.checkouts"
    val table = tableUtils.loadTable(nativeTable)
    table.show
    // val database = tableUtils.createDatabase("test_database")
    val allParts = tableUtils.partitions(nativeTable)
    println(allParts)
  }

  it should "integration testing bigquery external table" ignore {
    val externalTable = "default_iceberg.data.checkouts_parquet"

    val table = tableUtils.loadTable(externalTable)
    table.show
    // val database = tableUtils.createDatabase("test_database")
    val allParts = tableUtils.partitions(externalTable)
    println(allParts)
  }

  it should "integration testing bigquery partition pushdown" ignore {
    import spark.implicits._
    val iceberg = "data.checkouts_native"

    val singleFilter = tableUtils.loadTable(iceberg, List("ds = '2023-11-30'"))
    val multiFilter = tableUtils.loadTable(iceberg, List("ds = '2023-11-30'", "ds = '2023-11-30'"))
    assertEquals(singleFilter.select("user_id", "ds").as[(String, String)].collect.toList,
                 multiFilter.select("user_id", "ds").as[(String, String)].collect.toList)
  }

  it should "integration testing formats" ignore {
    val nativeTable = "default_iceberg.data.checkouts_native"
    val nativeFormat = FormatProvider.from(spark).readFormat(nativeTable)
    assertEquals(Some(BigQueryNative), nativeFormat)

    val nativeTableNoCat = "data.checkouts_native"
    val nativeFormatNoCat = FormatProvider.from(spark).readFormat(nativeTableNoCat)
    assertEquals(Some(BigQueryNative), nativeFormatNoCat)

    val icebergTable = "default_iceberg.data.quickstart_purchases_davidhan_v1_dev_davidhan"
    val icebergFormat = FormatProvider.from(spark).readFormat(icebergTable)
    assertEquals(Some(Iceberg), icebergFormat)

    val icebergTableNoCat = "data.quickstart_purchases_davidhan_v1_dev_davidhan"
    val icebergFormatNoCat = FormatProvider.from(spark).readFormat(icebergTableNoCat)
    assertEquals(Some(Iceberg), icebergFormatNoCat)

    val parts = icebergFormat.get.primaryPartitions(icebergTable, "ds", "")(spark)
    val partsNoCat = icebergFormat.get.primaryPartitions(icebergTableNoCat, "ds", "")(spark)
    assertEquals(parts, partsNoCat)

    val dneTable = "default_iceberg.data.dne"
    val dneFormat = FormatProvider.from(spark).readFormat(dneTable)
    assertTrue(dneFormat.isEmpty)
  }

  it should "integration testing bigquery partitions" ignore {
    // TODO(tchow): This test is ignored because it requires a running instance of the bigquery. Need to figure out stubbing locally.
    // to run, set `GOOGLE_APPLICATION_CREDENTIALS=<path_to_application_default_credentials.json>
    val externalPartitions = tableUtils.partitions("data.checkouts_parquet_partitioned")
    assertEquals(Seq("2023-11-30"), externalPartitions)
    val nativePartitions = tableUtils.partitions("data.purchases")
    assertEquals(
      Set(20231118, 20231122, 20231125, 20231102, 20231123, 20231119, 20231130, 20231101, 20231117, 20231110, 20231108,
          20231112, 20231115, 20231116, 20231113, 20231104, 20231103, 20231106, 20231121, 20231124, 20231128, 20231109,
          20231127, 20231129, 20231126, 20231114, 20231107, 20231111, 20231120, 20231105).map(_.toString),
      nativePartitions.toSet
    )

    val df = tableUtils.loadTable("`canary-443022.data`.purchases")
    df.show

    tableUtils.insertPartitions(df,
                                "data.tchow_test_iceberg",
                                Map("file_format" -> "PARQUET", "table_type" -> "iceberg"),
                                List("ds"))

    val icebergCols = spark.catalog.listColumns("data.tchow_test_iceberg")
    val externalCols = spark.catalog.listColumns("data.checkouts_parquet_partitioned")
    val nativeCols = spark.catalog.listColumns("data.purchases")

    val icebergPartitions = spark.sql("SELECT * FROM data.tchow_test_iceberg.partitions")

    val sqlDf = tableUtils.sql(s"""
        |SELECT ds FROM data.checkouts_parquet_partitioned -- external parquet
        |UNION ALL
        |SELECT ds FROM data.purchases -- bigquery native
        |UNION ALL
        |SELECT ds FROM data.tchow_test_iceberg -- external iceberg
        |""".stripMargin)
    sqlDf.show

  }

  it should "kryo serialization for ResolvingFileIO" in {
    val registrator = new ChrononKryoRegistrator()
    val kryo = new Kryo();
    kryo.setReferences(true);
    registrator.registerClasses(kryo)

    // Create an instance of ResolvingFileIO
    val original = new ResolvingFileIO();
    original.initialize(Map.empty[String, String].asJava)

    // Serialize the object
    val outputStream = new ByteArrayOutputStream();
    val output = new Output(outputStream);
    kryo.writeClassAndObject(output, original);
    output.close();

    // Deserialize the object
    val inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    val input = new Input(inputStream);
    val deserializedObj = kryo.readClassAndObject(input);
    input.close();
    original.close()
    deserializedObj.asInstanceOf[ResolvingFileIO].close()

    assertNotNull("Deserialized object should not be null", deserializedObj);
    assertTrue("Deserialized object should be an instance of ResolvingFileIO",
               deserializedObj.isInstanceOf[ResolvingFileIO]);
  }

  it should "kryo serialization for GCSFileIO" in {
    val registrator = new ChrononKryoRegistrator()
    val kryo = new Kryo();
    kryo.setReferences(true);
    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy))
    registrator.registerClasses(kryo)

    // Create an instance of GCSFileIO
    val original = new GCSFileIO();
    original.initialize(Map("k1" -> "v1").asJava)

    // Serialize the object
    val outputStream = new ByteArrayOutputStream();
    val output = new Output(outputStream);
    kryo.writeClassAndObject(output, original);
    output.close();

    // Deserialize the object
    val inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    val input = new Input(inputStream);
    val deserializedObj = kryo.readClassAndObject(input);
    input.close();

    assertNotNull("Deserialized object should not be null", deserializedObj);
    assertTrue("Deserialized object should be an instance of GCSFileIO", deserializedObj.isInstanceOf[GCSFileIO]);
    assertEquals(original.properties(), deserializedObj.asInstanceOf[GCSFileIO].properties())
  }

  it should "test CheckPartitions end-to-end partition parsing" in {
    import ai.chronon.spark.catalog.Format

    // Test the partition name parsing logic that CheckPartitions uses
    val testCases = Seq(
      ("catalog.dataset.table/ds=2024-01-01", ("catalog.dataset.table", List(("ds", "2024-01-01")))),
      ("dataset.table/ds=2024-01-01/region=us", ("dataset.table", List(("ds", "2024-01-01"), ("region", "us")))),
      ("simple_table/partition_col=value", ("simple_table", List(("partition_col", "value"))))
    )

    testCases.foreach { case (input, (expectedTable, expectedPartitions)) =>
      input.split("/").toList match {
        case fullTableName :: partitionParts if partitionParts.nonEmpty =>
          assertEquals(s"Table name should match for input: $input", expectedTable, fullTableName)
          val partitionSpec = partitionParts.mkString("/")
          val parsedSpec = Format.parseHiveStylePartition(partitionSpec)
          assertEquals(s"Partition spec should match for input: $input", expectedPartitions, parsedSpec)
        case _ => fail(s"Failed to parse input: $input")
      }
    }
  }

  it should "test BigQuery Metastore namespace compatibility in CheckPartitions" in {
    // Test case that would cause the original error: "data.gcp_purchases_12785_v1_test__0"
    val problematicTable = "catalog.data.gcp_purchases_12785_v1_test__0"

    // Test the partition checking logic that CheckPartitions uses
    val partitionNames = Seq(s"$problematicTable/ds=2024-01-01")
    val tablesToPartitionSpec = partitionNames.map { p =>
      p.split("/").toList match {
        case fullTableName :: partitionParts if partitionParts.nonEmpty =>
          val partitionSpec = partitionParts.mkString("/")
          (fullTableName, ai.chronon.spark.catalog.Format.parseHiveStylePartition(partitionSpec))
        case _ => fail(s"Failed to parse partition name: $p")
      }
    }

    // Verify the parsing worked correctly
    assertEquals(1, tablesToPartitionSpec.size)
    val (tableName, partitionSpec) = tablesToPartitionSpec.head
    assertEquals(problematicTable, tableName)
    assertEquals(List(("ds", "2024-01-01")), partitionSpec)

    // Test that SparkBQUtils.toIdentifierNoCatalog handles this correctly
    val identifier = SparkBQUtils.toIdentifierNoCatalog(tableName)(spark)
    assertEquals("gcp_purchases_12785_v1_test__0", identifier.name())
    assertEquals(1, identifier.namespace().length)
    assertEquals("data", identifier.namespace()(0))

    // Verify BigQuery Metastore compatibility: namespace should have at most 1 level
    assertTrue("Namespace should have at most 1 level for BigQuery Metastore compatibility",
               identifier.namespace().length <= 1)
  }

  it should "simulate CheckPartitions table reachability" in {
    // Test table reachability logic that CheckPartitions uses
    val testTables = Seq(
      "data.purchases",
      "data.checkouts_native",
      "default_iceberg.data.checkouts_parquet",
      "catalog.data.gcp_purchases_12785_v1_test__0"
    )

    testTables.foreach { tableName =>
      // This tests the table reachability check that CheckPartitions.run performs
      // We expect some tables to be reachable and others not, depending on the test environment
      val isReachable = tableUtils.tableReachable(tableName)

      // The key test is that this doesn't throw the BigQuery Metastore namespace error
      // Even if the table is not reachable, the parsing should work correctly
      val identifier = SparkBQUtils.toIdentifierNoCatalog(tableName)(spark)
      assertTrue(s"Should produce single-level namespace for table: $tableName, got: ${identifier.namespace().length}",
                 identifier.namespace().length <= 1)
    }
  }

  it should "test CheckPartitions partition spec validation" in {
    import ai.chronon.api.PartitionSpec
      import ai.chronon.spark.catalog.Format

    // Test various partition specifications that CheckPartitions might encounter
    val partitionTestCases = Seq(
      ("table/ds=2024-01-01", List(("ds", "2024-01-01"))),
      ("table/ds=2024-01-01/region=us", List(("ds", "2024-01-01"), ("region", "us"))),
      ("table/year=2024/month=01/day=01", List(("year", "2024"), ("month", "01"), ("day", "01"))),
      ("catalog.dataset.table/ds=2024-01-01", List(("ds", "2024-01-01")))
    )

    partitionTestCases.foreach { case (partitionName, expectedSpec) =>
      partitionName.split("/").toList match {
        case fullTableName :: partitionParts if partitionParts.nonEmpty =>
          val partitionSpecStr = partitionParts.mkString("/")
          val parsedSpec = Format.parseHiveStylePartition(partitionSpecStr)
          assertEquals(s"Partition spec should match for: $partitionName", expectedSpec, parsedSpec)

          // Test that we can create a PartitionSpec object (as CheckPartitions does)
          val partColumnName = parsedSpec.head._1
          val partitionSpec = PartitionSpec(
            partColumnName,
            tableUtils.partitionSpec.format,
            tableUtils.partitionSpec.spanMillis
          )
          assertEquals("Partition column should match", partColumnName, partitionSpec.column)

        case _ => fail(s"Failed to parse partition name: $partitionName")
      }
    }
  }
}
