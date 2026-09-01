package ai.chronon.spark.batch

import ai.chronon.aggregator.test.Column
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.planner.{JoinMergeNode, JoinPartNode, SourceWithFilterNode}
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils
import ai.chronon.spark.utils.{DataFrameGen, SparkTestBase}
import ai.chronon.spark.catalog.TableUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.junit.Assert._

import scala.collection.JavaConverters._

class MergeJobVersioningTest extends SparkTestBase {

  override protected def sparkConfs: Map[String, String] = Map(
    "spark.sql.catalog.spark_catalog.type" -> "hive",
    "spark.hadoop.iceberg.engine.hive.lock-enabled" -> "false"
  )

  // The full suite can reuse a SparkContext, in which case Spark does not reapply spark.hadoop.* builder configs.
  spark.sparkContext.hadoopConfiguration.set("iceberg.engine.hive.lock-enabled", "false")

  private implicit val tableUtils: TableUtils = TableUtils(spark)

  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val threeDaysAgo = tableUtils.partitionSpec.minus(today, new Window(3, TimeUnit.DAYS))
  private val start = tableUtils.partitionSpec.minus(today, new Window(7, TimeUnit.DAYS))
  private val monthAgo = tableUtils.partitionSpec.minus(today, new Window(30, TimeUnit.DAYS))
  private val yearAgo = tableUtils.partitionSpec.minus(today, new Window(365, TimeUnit.DAYS))

  private val namespace = "test_namespace_merge_job_versioning"
  SparkTestBase.createDatabase(spark, namespace)

  it should "reuse columns from production table when join part major versions match" in {
    val testName = "merge_job_versioning_test"

    // Create test data tables
    val leftSchema = List(
      Column("user", StringType, 10),
      Column("item", StringType, 10)
    )

    val sharedSchema = List(
      Column("user", StringType, 10),
      Column("price", DoubleType, 100)
    )

    val removedSchema = List(
      Column("user", StringType, 10),
      Column("quantity", LongType, 100)
    )

    val addedSchema = List(
      Column("user", StringType, 10),
      Column("rating", DoubleType, 5)
    )

    // Create tables
    val leftTable = s"$namespace.${testName}_left"
    val sharedTable = s"$namespace.${testName}_shared"
    val removedTable = s"$namespace.${testName}_removed"
    val addedTable = s"$namespace.${testName}_added"

    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $sharedTable")
    spark.sql(s"DROP TABLE IF EXISTS $removedTable")
    spark.sql(s"DROP TABLE IF EXISTS $addedTable")

    DataFrameGen.events(spark, leftSchema, 1000, 8).save(leftTable)
    DataFrameGen.events(spark, sharedSchema, 100, 60).save(sharedTable)
    DataFrameGen.events(spark, removedSchema, 100, 60).save(removedTable)
    DataFrameGen.events(spark, addedSchema, 100, 60).save(addedTable)

    // Create GroupBys
    val sharedGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = sharedTable,
          query = Builders.Query(selects = Builders.Selects("price"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "price")),
      metaData = Builders.MetaData(name = s"$testName.shared", namespace = namespace, team = "test_team")
    )

    val removedGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = removedTable,
          query = Builders.Query(selects = Builders.Selects("quantity"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.COUNT, inputColumn = "quantity")),
      metaData = Builders.MetaData(name = s"$testName.removed", namespace = namespace, team = "test_team")
    )

    val addedGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = addedTable,
          query = Builders.Query(selects = Builders.Selects("rating"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "rating")),
      metaData = Builders.MetaData(name = s"$testName.added", namespace = namespace, team = "test_team")
    )

    // Create join parts
    val sharedJoinPart = Builders.JoinPart(groupBy = sharedGroupBy, prefix = "shared").setUseLongNames(false)
    val removedJoinPart = Builders.JoinPart(groupBy = removedGroupBy, prefix = "removed").setUseLongNames(false)
    val addedJoinPart = Builders.JoinPart(groupBy = addedGroupBy, prefix = "added").setUseLongNames(false)

    // Create v0 join (with shared and removed)
    val joinV0 = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(sharedJoinPart, removedJoinPart),
        metaData = Builders.MetaData(name = testName + "__0", version = 0, namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    // Create v1 join (with shared and added)
    val joinV1 = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(sharedJoinPart, addedJoinPart),
        metaData = Builders.MetaData(name = testName + "__1", version = 1, namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    val dateRange = new DateRange().setStartDate(start).setEndDate(threeDaysAgo)

    // Set column hashes on v0 join metadata so MergeJob handles table properties automatically
    val productionColumnHashes = Map(
      "user" -> "hash_user",
      "item" -> "hash_item",
      "ts" -> "hash_ts",
      "shared_user_price_sum" -> "hash_shared_user_price_sum",
      "removed_user_quantity_count" -> "hash_removed_user_quantity_count"
    )
    joinV0.metaData.setColumnHashes(productionColumnHashes.asJava)

    // Step 1: Run v0 join modularly to create production table
    // 1a. Run source job for v0
    val v0SourceOutputTable = JoinUtils.computeFullLeftSourceTableName(joinV0)
    val v0SourceParts = v0SourceOutputTable.split("\\.", 2)
    val v0SourceNamespace = v0SourceParts(0)
    val v0SourceName = v0SourceParts(1)

    val v0SourceMetaData = new MetaData()
      .setName(v0SourceName)
      .setOutputNamespace(v0SourceNamespace)

    val v0LeftSourceWithFilter = new SourceWithFilterNode().setSource(joinV0.left)
    val v0SourceRunner = new SourceJob(v0LeftSourceWithFilter, v0SourceMetaData, dateRange)
    v0SourceRunner.run()

    // 1b. Run join part jobs for v0 (shared and removed)
    for (joinPart <- Seq(sharedJoinPart, removedJoinPart)) {
      val partTableName = RelevantLeftForJoinPart.partTableName(joinV0, joinPart)
      val partMetaData = new MetaData()
        .setName(partTableName)
        .setOutputNamespace(joinV0.metaData.outputNamespace)

      val joinPartNode = new JoinPartNode()
        .setLeftSourceTable(v0SourceOutputTable)
        .setLeftDataModel(joinV0.getLeft.dataModel)
        .setJoinPart(joinPart)

      val joinPartJob = new JoinPartJob(joinPartNode, partMetaData, dateRange)
      joinPartJob.run()
    }

    // 1c. Run merge job for v0
    val v0MergeNode = new JoinMergeNode()
      .setJoin(joinV0)

    val v0MergeMetaData = new MetaData()
      .setName(joinV0.metaData.name)
      .setOutputNamespace(namespace)

    val v0MergeJob = new MergeJob(v0MergeNode, v0MergeMetaData, dateRange, Seq(sharedJoinPart, removedJoinPart))
    v0MergeJob.run()

    // Step 2: Manually modify production table with literal values, maintaining partitioning
    val productionTable = joinV0.metaData.outputTable
    val existingProductionData = tableUtils.scanDf(null, productionTable, None)

    existingProductionData.show()
    print(existingProductionData.schema.pretty)

    val sharedColumnName = s"shared_user_price_sum"
    val removedColumnName = s"removed_user_quantity_count"

    // Replace with literal values for testing, maintaining original structure
    val productionDataWithLiterals =
      existingProductionData //.drop(sharedColumnName, removedColumnName) // Remove existing columns
        .withColumn(sharedColumnName, lit(999.0)) // Literal value we expect to be reused
        .withColumn(removedColumnName, lit(42L)) // This should not appear in v1 result

    productionDataWithLiterals.show()
    print(productionDataWithLiterals.schema.pretty)

    // Use proper partition overwrite to maintain partitioning
    productionDataWithLiterals.write
      .mode(SaveMode.Overwrite)
      .insertInto(productionTable)

    // Step 3: Run source job for v1
    val sourceOutputTable = JoinUtils.computeFullLeftSourceTableName(joinV1)
    val sourceParts = sourceOutputTable.split("\\.", 2)
    val sourceNamespace = sourceParts(0)
    val sourceName = sourceParts(1)

    val sourceMetaData = new MetaData()
      .setName(sourceName)
      .setOutputNamespace(sourceNamespace)

    tableUtils.sql(f"SELECT * from $leftTable").show()
    tableUtils.sql(f"SELECT distinct ds from $leftTable order by ds desc").show(100)

    val leftSourceWithFilter = new SourceWithFilterNode().setSource(joinV1.left)
    val sourceRunner = new SourceJob(leftSourceWithFilter, sourceMetaData, dateRange)
    sourceRunner.run()

    // Step 4: Run join part job for the added GroupBy only (shared will be reused from production)
    val addedPartTableName = RelevantLeftForJoinPart.partTableName(joinV1, addedJoinPart)
    val addedPartFullTableName = RelevantLeftForJoinPart.fullPartTableName(joinV1, addedJoinPart)

    val addedPartMetaData = new MetaData()
      .setName(addedPartTableName)
      .setOutputNamespace(joinV1.metaData.outputNamespace)

    val addedJoinPartNode = new JoinPartNode()
      .setLeftSourceTable(sourceOutputTable)
      .setLeftDataModel(joinV1.getLeft.dataModel)
      .setJoinPart(addedJoinPart)

    val addedJoinPartJob = new JoinPartJob(addedJoinPartNode, addedPartMetaData, dateRange)
    addedJoinPartJob.run()

    // Step 5: Run MergeJob with production join reference
    val mergeNode = new JoinMergeNode()
      .setJoin(joinV1)
      .setProductionJoin(joinV0)

    val mergeMetaData = new MetaData()
      .setName(joinV1.metaData.name)
      .setOutputNamespace(namespace)

    val mergeJob = new MergeJob(mergeNode, mergeMetaData, dateRange, Seq(sharedJoinPart, addedJoinPart))

    // Set column hashes in the new join's metadata (v1)
    joinV1.metaData.setColumnHashes(
      Map(
        "user" -> "hash_user",
        "item" -> "hash_item",
        "ts" -> "hash_ts",
        "shared_user_price_sum" -> "hash_shared_user_price_sum", // This should match production table
        "added_user_rating_average" -> "hash_added_user_rating_average" // This won't be in production
      ).asJava)

    mergeJob.run()

    // Step 6: Verify results
    val resultTable = joinV1.metaData.outputTable
    val result = tableUtils.scanDf(null, resultTable, None)

    val resultRows = result.collect()
    assertTrue("Should have results", resultRows.length > 0)
    result.show()

    // Verify that shared column was reused from production (literal value 999.0)
    assertTrue(s"Result should contain reused column $sharedColumnName", result.columns.contains(sharedColumnName))

    val reusedValues = result.select(sharedColumnName).distinct().collect()
    assertEquals("Should have exactly one distinct value for reused column", 1, reusedValues.length)
    assertEquals("Reused column should have literal value from production",
                 999.0,
                 reusedValues(0).getAs[Double](sharedColumnName),
                 0.01)

    // Verify that added column was computed normally
    val addedColumnName = s"added_user_rating_average"
    assertTrue(s"Result should contain computed column $addedColumnName", result.columns.contains(addedColumnName))

    val addedValues = result.select(addedColumnName).filter(col(addedColumnName).isNotNull).collect()
    assertTrue("Added column should have non-null computed values", addedValues.length > 0)

    // Verify that removed column is not present in result
    assertFalse(s"Removed column should not be present", result.columns.contains(removedColumnName))

    // Verify essential columns are present
    val expectedEssentialColumns = Set("user", "item", "ds", "ts")
    val actualColumns = result.columns.toSet
    assertTrue("Essential columns should be present", expectedEssentialColumns.subsetOf(actualColumns))

    println(s"Test passed! Result schema: ${result.columns.mkString(", ")}")
    println(s"Reused column distinct values: ${reusedValues.length}")
    println(s"Computed column non-null values: ${addedValues.length}")
  }

  it should "archive current table and reuse unchanged columns when a group_by changes" in {
    val testName = "merge_job_versioning_test_2"
    val namespace = "modify_gb_versioning"
    SparkTestBase.createDatabase(spark, namespace)

    // Step 1 -- create a join with two group bys and run it
    val leftSchema = List(
      Column("user", StringType, 10),
      Column("product", StringType, 10)
    )

    val userSchema = List(
      Column("user", StringType, 10),
      Column("price", DoubleType, 100)
    )

    val productSchema = List(
      Column("product", StringType, 10),
      Column("rating", DoubleType, 5)
    )

    // Create tables
    val leftTable = s"$namespace.${testName}_left"
    val userTable = s"$namespace.${testName}_user"
    val productTable = s"$namespace.${testName}_product"

    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $userTable")
    spark.sql(s"DROP TABLE IF EXISTS $productTable")

    DataFrameGen.events(spark, leftSchema, 1500, 8).save(leftTable)
    DataFrameGen.events(spark, userSchema, 100, 60).save(userTable)
    DataFrameGen.events(spark, productSchema, 100, 60).save(productTable)

    // Create GroupBys for original join
    val userGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = userTable,
          query = Builders.Query(selects = Builders.Selects("price"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "price")),
      metaData = Builders.MetaData(name = s"$testName.user", namespace = namespace, team = "test_team")
    )

    val productGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = productTable,
          query = Builders.Query(selects = Builders.Selects("rating"), startPartition = yearAgo)
        )),
      keyColumns = Seq("product"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "rating")),
      metaData = Builders.MetaData(name = s"$testName.product", namespace = namespace, team = "test_team")
    )

    // Create join parts
    val userJoinPart = Builders.JoinPart(groupBy = userGroupBy, prefix = "user").setUseLongNames(false)
    val productJoinPart = Builders.JoinPart(groupBy = productGroupBy, prefix = "product").setUseLongNames(false)

    // Create original join
    val originalJoin = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(userJoinPart, productJoinPart),
        metaData = Builders.MetaData(name = testName, namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    val dateRange = new DateRange().setStartDate(start).setEndDate(threeDaysAgo)

    // Set column hashes on original join metadata so MergeJob handles table properties automatically
    val originalColumnHashes = Map(
      "user" -> "hash_user",
      "product" -> "hash_product",
      "ts" -> "hash_ts",
      "user_user_price_sum" -> "hash_user_user_price_sum",
      "product_product_rating_average" -> "hash_product_product_rating_average"
    )
    originalJoin.metaData.setColumnHashes(originalColumnHashes.asJava)

    // Run original join modularly to create production table
    // 1a. Run source job for original join
    val originalSourceOutputTable = JoinUtils.computeFullLeftSourceTableName(originalJoin)
    val originalSourceParts = originalSourceOutputTable.split("\\.", 2)
    val originalSourceNamespace = originalSourceParts(0)
    val originalSourceName = originalSourceParts(1)

    val originalSourceMetaData = new MetaData()
      .setName(originalSourceName)
      .setOutputNamespace(originalSourceNamespace)

    val originalLeftSourceWithFilter = new SourceWithFilterNode().setSource(originalJoin.left)
    val originalSourceRunner = new SourceJob(originalLeftSourceWithFilter, originalSourceMetaData, dateRange)
    originalSourceRunner.run()

    // 1b. Run join part jobs for original join (user and product)
    for (joinPart <- Seq(userJoinPart, productJoinPart)) {
      val partTableName = RelevantLeftForJoinPart.partTableName(originalJoin, joinPart)
      val partMetaData = new MetaData()
        .setName(partTableName)
        .setOutputNamespace(originalJoin.metaData.outputNamespace)

      val joinPartNode = new JoinPartNode()
        .setLeftSourceTable(originalSourceOutputTable)
        .setLeftDataModel(originalJoin.getLeft.dataModel)
        .setJoinPart(joinPart)

      val joinPartJob = new JoinPartJob(joinPartNode, partMetaData, dateRange)
      joinPartJob.run()
    }

    // 1c. Run merge job for original join
    val originalMergeNode = new JoinMergeNode()
      .setJoin(originalJoin)

    val originalMergeMetaData = new MetaData()
      .setName(originalJoin.metaData.name)
      .setOutputNamespace(namespace)

    val originalMergeJob =
      new MergeJob(originalMergeNode, originalMergeMetaData, dateRange, Seq(userJoinPart, productJoinPart))
    originalMergeJob.run()

    // Verify original join output
    val productionTable = originalJoin.metaData.outputTable

    // Step 2 -- Create a new join with the same name, but in this one use a modified version of one of the GroupBys
    // The modified version can use a filter on the source query. Be sure to make the semantic hashes of the new
    // columns different from the old on this new join. The name and version can be the same.

    // Create modified user GroupBy with different filter (semantic hash will be different)
    val modifiedUserGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = userTable,
          query = Builders.Query(
            selects = Builders.Selects("price"),
            wheres = Seq("price > 50"), // Different filter - changes semantic hash
            startPartition = yearAgo
          )
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "price")),
      metaData = Builders.MetaData(name = s"$testName.user", namespace = namespace, team = "test_team")
    )

    // Product GroupBy remains unchanged
    val unchangedProductJoinPart =
      Builders.JoinPart(groupBy = productGroupBy, prefix = "product").setUseLongNames(false)
    val modifiedUserJoinPart = Builders.JoinPart(groupBy = modifiedUserGroupBy, prefix = "user").setUseLongNames(false)

    // Create new join with same name but modified GroupBy
    val newJoin = Builders
      .Join(
        left = Builders.Source.events(table = leftTable, query = Builders.Query(startPartition = start)),
        joinParts = Seq(modifiedUserJoinPart, unchangedProductJoinPart),
        metaData = Builders.MetaData(name = testName, namespace = namespace, team = "test_team") // Same name
      )
      .setUseLongNames(false)

    // Set different column hashes for new join (modified user part has different hash)
    newJoin.metaData.setColumnHashes(
      Map(
        "user" -> "hash_user",
        "product" -> "hash_product",
        "ts" -> "hash_ts",
        "user_user_price_sum" -> "hash_modified_user_user_price_sum", // Different hash due to filter change
        "product_product_rating_average" -> "hash_product_product_rating_average" // Same hash (unchanged)
      ).asJava)

    // Step 3 -- Call MergeJob.archiveOutputTableIfRequired() directly on the new Join
    val mergeNode = new JoinMergeNode()
      .setJoin(newJoin)

    val mergeMetaData = new MetaData()
      .setName(newJoin.metaData.name)
      .setOutputNamespace(namespace)

    val mergeJob =
      new MergeJob(mergeNode, mergeMetaData, dateRange, Seq(modifiedUserJoinPart, unchangedProductJoinPart))

    // Archive the existing table
    mergeJob.archiveOutputTableIfRequired()

    // Step 4 -- Ensure that the archive table exists, then set the columns that should be shared
    // (i.e. the ones from the unchanged GroupBy) to literal values
    val archiveTable = mergeJob.archiveReuseTable

    // Check if archive table was created (it should follow a naming pattern)
    val archiveTableExists = tableUtils.tableReachable(archiveTable)

    assertTrue("Archive table should be created", archiveTableExists)

    // Modify archive table to set literal values for columns that should be reused
    val archiveData = tableUtils.scanDf(null, archiveTable, None)
    println(s"Archive table:")
    archiveData.show()

    val modifiedArchiveData = archiveData
      .withColumn("product_product_rating_average", lit(888.0)) // Literal value we expect to be reused

    modifiedArchiveData.write
      .mode(SaveMode.Overwrite)
      .insertInto(archiveTable)

    println(s"Modified archive table:")
    modifiedArchiveData.show()

    // Step 5 -- run the new MergeJob (with no table passed as production join, since we're only relying on archive reuse here)
    // Run source and join part jobs first
    val sourceOutputTable = JoinUtils.computeFullLeftSourceTableName(newJoin)
    val sourceParts = sourceOutputTable.split("\\.", 2)
    val sourceNamespace = sourceParts(0)
    val sourceName = sourceParts(1)

    val sourceMetaData = new MetaData()
      .setName(sourceName)
      .setOutputNamespace(sourceNamespace)

    // Run source job for new join
    val leftSourceWithFilter = new SourceWithFilterNode().setSource(newJoin.left)
    val sourceRunner = new SourceJob(leftSourceWithFilter, sourceMetaData, dateRange)
    sourceRunner.run()

    // Run join part jobs for both parts
    for (joinPart <- Seq(modifiedUserJoinPart, unchangedProductJoinPart)) {
      val partTableName = RelevantLeftForJoinPart.partTableName(newJoin, joinPart)
      val partMetaData = new MetaData()
        .setName(partTableName)
        .setOutputNamespace(newJoin.metaData.outputNamespace)

      val joinPartNode = new JoinPartNode()
        .setLeftSourceTable(sourceOutputTable)
        .setLeftDataModel(newJoin.getLeft.dataModel)
        .setJoinPart(joinPart)

      val joinPartJob = new JoinPartJob(joinPartNode, partMetaData, dateRange)
      joinPartJob.run()
    }

    // Run merge job
    mergeJob.run()

    // Step 6 -- assert that the result contains the reused columns with the literal values, and that the new columns are computed correctly
    val resultTable = newJoin.metaData.outputTable
    val result = tableUtils.scanDf(null, resultTable, None)

    val resultRows = result.collect()
    assertTrue("Should have results", resultRows.length > 0)
    result.show()

    // Verify that unchanged product column was reused from archive (literal value 888.0)
    val productColumnName = "product_product_rating_average"
    assertTrue(s"Result should contain reused column $productColumnName", result.columns.contains(productColumnName))

    val reusedValues = result.select(productColumnName).distinct().collect()
    assertEquals("Should have exactly one distinct value for reused column", 1, reusedValues.length)
    assertEquals("Reused column should have literal value from archive",
                 888.0,
                 reusedValues(0).getAs[Double](productColumnName),
                 0.01)

    // Verify that modified user column was computed normally (not reused)
    val userColumnName = "user_user_price_sum"
    assertTrue(s"Result should contain computed column $userColumnName", result.columns.contains(userColumnName))

    val userValues = result.select(userColumnName).filter(col(userColumnName).isNotNull).collect()
    assertTrue("User column should have non-null computed values", userValues.length > 0)

    // User values should NOT be the literal value (should be computed)
    val userDistinctValues = result.select(userColumnName).distinct().collect()
    assertTrue("User column should not have the literal archive value",
               userDistinctValues.length > 1 || userDistinctValues(0).getAs[Double](userColumnName) != 888.0)

    println(s"Test passed! Archive table: ${archiveTable}")
    println(s"Result schema: ${result.columns.mkString(", ")}")
    println(s"Reused column distinct values: ${reusedValues.length}")
    println(s"Computed column non-null values: ${userValues.length}")
  }

  it should "archive current table but not reuse any columns when left time column changes" in {
    val testName = "merge_job_versioning_test_3"
    val namespace = "time_column_change_versioning"
    SparkTestBase.createDatabase(spark, namespace)

    // Step 1 -- create a join with two group bys and run it
    val leftSchema = List(
      Column("user", StringType, 10),
      Column("product", StringType, 10)
    )

    val userSchema = List(
      Column("user", StringType, 10),
      Column("price", DoubleType, 100)
    )

    val productSchema = List(
      Column("product", StringType, 10),
      Column("rating", DoubleType, 5)
    )

    // Create tables
    val leftTable = s"$namespace.${testName}_left"
    val userTable = s"$namespace.${testName}_user"
    val productTable = s"$namespace.${testName}_product"

    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $userTable")
    spark.sql(s"DROP TABLE IF EXISTS $productTable")

    DataFrameGen.events(spark, leftSchema, 1500, 8).save(leftTable)
    DataFrameGen.events(spark, userSchema, 100, 60).save(userTable)
    DataFrameGen.events(spark, productSchema, 100, 60).save(productTable)

    // Create GroupBys for original join
    val userGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = userTable,
          query = Builders.Query(selects = Builders.Selects("price"), startPartition = yearAgo)
        )),
      keyColumns = Seq("user"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.SUM, inputColumn = "price")),
      metaData = Builders.MetaData(name = s"$testName.user", namespace = namespace, team = "test_team")
    )

    val productGroupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = productTable,
          query = Builders.Query(selects = Builders.Selects("rating"), startPartition = yearAgo)
        )),
      keyColumns = Seq("product"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "rating")),
      metaData = Builders.MetaData(name = s"$testName.product", namespace = namespace, team = "test_team")
    )

    // Create join parts
    val userJoinPart = Builders.JoinPart(groupBy = userGroupBy, prefix = "user").setUseLongNames(false)
    val productJoinPart = Builders.JoinPart(groupBy = productGroupBy, prefix = "product").setUseLongNames(false)

    // Create original join with simple timestamp
    val originalJoin = Builders
      .Join(
        left = Builders.Source.events(
          table = leftTable,
          query = Builders.Query(
            selects = Builders.Selects("user", "product"),
            timeColumn = "ts", // Simple time column
            startPartition = start
          )
        ),
        joinParts = Seq(userJoinPart, productJoinPart),
        metaData = Builders.MetaData(name = testName, namespace = namespace, team = "test_team")
      )
      .setUseLongNames(false)

    val dateRange = new DateRange().setStartDate(start).setEndDate(threeDaysAgo)

    // Set column hashes on original join metadata (all columns have original hashes)
    val originalColumnHashes = Map(
      "user" -> "hash_user_original",
      "product" -> "hash_product_original",
      "user_user_price_sum" -> "hash_user_user_price_sum_original",
      "product_product_rating_average" -> "hash_product_product_rating_average_original"
    )
    originalJoin.metaData.setColumnHashes(originalColumnHashes.asJava)

    // Run original join modularly to create production table
    // 1a. Run source job for original join
    val originalSourceOutputTable = JoinUtils.computeFullLeftSourceTableName(originalJoin)
    val originalSourceParts = originalSourceOutputTable.split("\\.", 2)
    val originalSourceNamespace = originalSourceParts(0)
    val originalSourceName = originalSourceParts(1)

    val originalSourceMetaData = new MetaData()
      .setName(originalSourceName)
      .setOutputNamespace(originalSourceNamespace)

    val originalLeftSourceWithFilter = new SourceWithFilterNode().setSource(originalJoin.left)
    val originalSourceRunner = new SourceJob(originalLeftSourceWithFilter, originalSourceMetaData, dateRange)
    originalSourceRunner.run()

    // 1b. Run join part jobs for original join (user and product)
    for (joinPart <- Seq(userJoinPart, productJoinPart)) {
      val partTableName = RelevantLeftForJoinPart.partTableName(originalJoin, joinPart)
      val partMetaData = new MetaData()
        .setName(partTableName)
        .setOutputNamespace(originalJoin.metaData.outputNamespace)

      val joinPartNode = new JoinPartNode()
        .setLeftSourceTable(originalSourceOutputTable)
        .setLeftDataModel(originalJoin.getLeft.dataModel)
        .setJoinPart(joinPart)

      val joinPartJob = new JoinPartJob(joinPartNode, partMetaData, dateRange)
      joinPartJob.run()
    }

    // 1c. Run merge job for original join
    val originalMergeNode = new JoinMergeNode()
      .setJoin(originalJoin)

    val originalMergeMetaData = new MetaData()
      .setName(originalJoin.metaData.name)
      .setOutputNamespace(namespace)

    val originalMergeJob =
      new MergeJob(originalMergeNode, originalMergeMetaData, dateRange, Seq(userJoinPart, productJoinPart))
    originalMergeJob.run()

    // Verify original join output
    val productionTable = originalJoin.metaData.outputTable

    // Step 2 -- Create a new join with the same name but different time column expression
    // This will change ALL semantic hashes including left side columns

    // Create new join with different time column expression
    val newJoin = Builders
      .Join(
        left = Builders.Source.events(
          table = leftTable,
          query = Builders.Query(
            selects = Builders.Selects("user", "product"),
            timeColumn = "CAST(ts AS DOUBLE)", // Different time column expression - changes ALL semantic hashes
            startPartition = start
          )
        ),
        joinParts = Seq(userJoinPart, productJoinPart),
        metaData = Builders.MetaData(name = testName, namespace = namespace, team = "test_team") // Same name
      )
      .setUseLongNames(false)

    // Set different column hashes for new join (ALL columns have different hashes due to time column change)
    newJoin.metaData.setColumnHashes(
      Map(
        "user" -> "hash_user_new_time", // Different hash due to time column change
        "product" -> "hash_product_new_time", // Different hash due to time column change
        "user_user_price_sum" -> "hash_user_user_price_sum_new_time", // Different hash due to time column change
        "product_product_rating_average" -> "hash_product_product_rating_average_new_time" // Different hash due to time column change
      ).asJava)

    // Step 3 -- Call MergeJob.archiveOutputTableIfRequired() directly on the new Join
    val mergeNode = new JoinMergeNode()
      .setJoin(newJoin)

    val mergeMetaData = new MetaData()
      .setName(newJoin.metaData.name)
      .setOutputNamespace(namespace)

    val mergeJob =
      new MergeJob(mergeNode, mergeMetaData, dateRange, Seq(userJoinPart, productJoinPart))

    // Archive the existing table
    mergeJob.archiveOutputTableIfRequired()

    // Step 4 -- Ensure that the archive table exists, then set literal values
    val archiveTable = mergeJob.archiveReuseTable

    // Check if archive table was created
    val archiveTableExists = tableUtils.tableReachable(archiveTable)
    assertTrue("Archive table should be created", archiveTableExists)

    // Modify archive table to set literal values for columns
    val archiveData = tableUtils.scanDf(null, archiveTable, None)
    println(s"Archive table:")
    archiveData.show()

    val modifiedArchiveData = archiveData
      .withColumn("user_user_price_sum", lit(777.0)) // Literal value that should NOT be reused
      .withColumn("product_product_rating_average", lit(888.0)) // Literal value that should NOT be reused

    modifiedArchiveData.write
      .mode(SaveMode.Overwrite)
      .insertInto(archiveTable)

    println(s"Modified archive table:")
    modifiedArchiveData.show()

    // Step 5 -- run the new MergeJob - should NOT reuse any columns due to time column change
    // Run source and join part jobs first
    val sourceOutputTable = JoinUtils.computeFullLeftSourceTableName(newJoin)
    val sourceParts = sourceOutputTable.split("\\.", 2)
    val sourceNamespace = sourceParts(0)
    val sourceName = sourceParts(1)

    val sourceMetaData = new MetaData()
      .setName(sourceName)
      .setOutputNamespace(sourceNamespace)

    // Run source job for new join
    val leftSourceWithFilter = new SourceWithFilterNode().setSource(newJoin.left)
    val sourceRunner = new SourceJob(leftSourceWithFilter, sourceMetaData, dateRange)
    sourceRunner.run()

    // Run join part jobs for both parts
    for (joinPart <- Seq(userJoinPart, productJoinPart)) {
      val partTableName = RelevantLeftForJoinPart.partTableName(newJoin, joinPart)
      val partMetaData = new MetaData()
        .setName(partTableName)
        .setOutputNamespace(newJoin.metaData.outputNamespace)

      val joinPartNode = new JoinPartNode()
        .setLeftSourceTable(sourceOutputTable)
        .setLeftDataModel(newJoin.getLeft.dataModel)
        .setJoinPart(joinPart)

      val joinPartJob = new JoinPartJob(joinPartNode, partMetaData, dateRange)
      joinPartJob.run()
    }

    // Run merge job
    mergeJob.run()

    // Step 6 -- assert that NO columns were reused (no literal values should appear in result)
    val resultTable = newJoin.metaData.outputTable
    val result = tableUtils.scanDf(null, resultTable, None)

    val resultRows = result.collect()
    assertTrue("Should have results", resultRows.length > 0)
    result.show()

    // Verify that NO columns were reused (no literal values from archive should appear)
    val userColumnName = "user_user_price_sum"
    val productColumnName = "product_product_rating_average"

    assertTrue(s"Result should contain computed column $userColumnName", result.columns.contains(userColumnName))
    assertTrue(s"Result should contain computed column $productColumnName", result.columns.contains(productColumnName))

    // Check that literal values from archive do NOT appear in the result
    val userValues = result.select(userColumnName).filter(col(userColumnName).isNotNull).collect()
    val productValues = result.select(productColumnName).filter(col(productColumnName).isNotNull).collect()

    assertTrue("User column should have computed values", userValues.length > 0)
    assertTrue("Product column should have computed values", productValues.length > 0)

    // Most importantly: verify that the literal values from archive do NOT appear in result
    val userLiteralCount = result.select(userColumnName).filter(col(userColumnName) === 777.0).count()
    val productLiteralCount = result.select(productColumnName).filter(col(productColumnName) === 888.0).count()

    assertEquals("No literal values should be reused from archive for user column", 0, userLiteralCount)
    assertEquals("No literal values should be reused from archive for product column", 0, productLiteralCount)

    // Verify that values are actually computed (not reused)
    val userDistinctValues = result.select(userColumnName).distinct().collect()
    val productDistinctValues = result.select(productColumnName).distinct().collect()

    assertTrue(
      "User column should have varied computed values (not just literal)",
      userDistinctValues.length > 1 || !userDistinctValues.exists(_.getAs[Double](userColumnName) == 777.0)
    )
    assertTrue(
      "Product column should have varied computed values (not just literal)",
      productDistinctValues.length > 1 || !productDistinctValues.exists(_.getAs[Double](productColumnName) == 888.0)
    )

    println(s"Test passed! Archive table: ${archiveTable}")
    println(s"Result schema: ${result.columns.mkString(", ")}")
    println(s"User column distinct values: ${userDistinctValues.length}")
    println(s"Product column distinct values: ${productDistinctValues.length}")
    println(s"No literal values reused due to time column change - all columns computed fresh")
  }
}
