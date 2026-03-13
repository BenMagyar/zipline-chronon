package ai.chronon.spark.utils

import ai.chronon.api.{Constants, LongType, StringType, StructField, StructType}
import ai.chronon.spark.catalog.{CreationUtils, TableUtils}
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.utils.TestUtils.makeDf
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class SemanticUtilsTest extends AnyFlatSpec with BeforeAndAfterEach {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  lazy val spark: SparkSession = SparkSessionBuilder.build("semantic_utils_test", local = true)
  implicit val tableUtils: TableUtils = TableUtils(spark)
  private val semanticUtils = new SemanticUtils(tableUtils)
  private val namespace = "semantic_utils_test"
  private val testDb = namespace
  override def beforeEach(): Unit = {
    SparkTestBase.createDatabase(spark, namespace)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    try {
      spark.sql(s"DROP DATABASE IF EXISTS $namespace CASCADE")
    } finally {
      super.afterEach()
    }
  }

  private def createTestTable(tableName: String, semanticHash: Option[String] = None): Unit = {
    val schema = StructType(
      tableName,
      Array(
        StructField("id", LongType),
        StructField("name", StringType),
        StructField("ds", StringType)
      )
    )

    val df = makeDf(
      spark,
      schema,
      List(Row(1L, "test", "2024-01-01"))
    )

    tableUtils.insertPartitions(df, tableName, partitionColumns = List("ds"))

    // Set semantic hash if provided
    semanticHash.foreach { hash =>
      val currentProps = tableUtils.getTableProperties(tableName).getOrElse(Map.empty)
      val alterStmt = CreationUtils.alterTablePropertiesSql(
        tableName,
        currentProps + (Constants.SemanticHashKey -> hash)
      )
      tableUtils.sql(alterStmt)
    }
  }

  private def getSemanticHashFromTable(tableName: String): Option[String] = {
    tableUtils.getTableProperties(tableName).flatMap(_.get(Constants.SemanticHashKey))
  }

  it should "checkSemanticHashAndArchive should not archive when semantic hash matches" in {
    val tableName = s"$testDb.test_table_same_hash"
    val semanticHash = "hash123"

    createTestTable(tableName, Some(semanticHash))
    assertTrue(spark.catalog.tableExists(tableName))

    // Should not archive when hash matches
    val archivedTableOpt = semanticUtils.checkSemanticHashAndArchive(tableName, semanticHash)

    // Should return None since table was not archived
    assertEquals(None, archivedTableOpt)

    // Table should still exist
    assertTrue(spark.catalog.tableExists(tableName))
    assertEquals(Some(semanticHash), getSemanticHashFromTable(tableName))
  }

  it should "checkSemanticHashAndArchive should archive when semantic hash is different" in {
    val tableName = s"$testDb.test_table_diff_hash"
    val oldHash = "hash123"
    val newHash = "hash456"

    createTestTable(tableName, Some(oldHash))
    assertTrue(spark.catalog.tableExists(tableName))

    // Should archive when hash is different
    val archivedTableOpt = semanticUtils.checkSemanticHashAndArchive(tableName, newHash)

    // Original table should be archived to the reuse table
    assertFalse(spark.catalog.tableExists(tableName))

    // Should return the reuse table name
    val expectedReuseTable = tableName + Constants.archiveReuseTableSuffix
    assertEquals(Some(expectedReuseTable), archivedTableOpt)
    assertTrue(s"Archived reuse table $expectedReuseTable should exist", spark.catalog.tableExists(expectedReuseTable))
  }

  it should "checkSemanticHashAndArchive should not archive when table has no semantic hash" in {
    val tableName = s"$testDb.test_table_no_hash"
    val newHash = "hash789"

    createTestTable(tableName, None)
    assertTrue(spark.catalog.tableExists(tableName))

    // Should not archive when no hash exists — hash will be set after successful job completion
    val archivedTableOpt = semanticUtils.checkSemanticHashAndArchive(tableName, newHash)

    // Should return None since table was not archived
    assertEquals(None, archivedTableOpt)

    // Table should still exist
    assertTrue(spark.catalog.tableExists(tableName))
  }

  it should "checkSemanticHashAndArchive should handle non-existent table gracefully" in {
    val tableName = s"$testDb.non_existent_table"
    val semanticHash = "hash999"

    // Verify table does not exist before the call
    assertFalse(spark.catalog.tableExists(tableName))

    // Should handle non-existent table without throwing and return None
    // since getTableProperties returns None for non-existent tables
    val result = semanticUtils.checkSemanticHashAndArchive(tableName, semanticHash)

    // Should return None - no archiving occurs for non-existent tables
    assertEquals(None, result)

    // Table should still not exist (no side effects)
    assertFalse(spark.catalog.tableExists(tableName))
  }

  it should "archive to shelf when reuse table already exists" in {
    val tableName = s"$testDb.test_shelf_behavior"
    val hash1 = "hash_v1"
    val hash2 = "hash_v2"
    val hash3 = "hash_v3"

    // Step 1: Create and archive first table
    createTestTable(tableName, Some(hash1))
    val reuseTable = tableName + Constants.archiveReuseTableSuffix
    val archivedOpt1 = semanticUtils.checkSemanticHashAndArchive(tableName, hash2)
    assertEquals(Some(reuseTable), archivedOpt1)
    assertFalse(spark.catalog.tableExists(tableName))
    assertTrue(spark.catalog.tableExists(reuseTable))

    // Step 2: Create new table with different hash and archive again
    createTestTable(tableName, Some(hash2))
    val archivedOpt2 = semanticUtils.checkSemanticHashAndArchive(tableName, hash3)

    // Should still return the reuse table
    assertEquals(Some(reuseTable), archivedOpt2)

    // Original table should be archived to reuse
    assertFalse(spark.catalog.tableExists(tableName))
    assertTrue(spark.catalog.tableExists(reuseTable))

    // Old reuse table should have been moved to shelf with timestamp suffix
    // Find the shelf table (should match pattern: tableName_archive_YYYYMMDDHHMMSS)
    val allTables = spark.catalog.listTables(testDb).collect().map(_.name)
    val shelfTables = allTables.filter(t =>
      t.startsWith(s"test_shelf_behavior_archive_") &&
      t != "test_shelf_behavior_archive_reuse" &&
      t.matches("test_shelf_behavior_archive_\\d{14}")
    )

    // Should have exactly one shelf table
    assertEquals(1, shelfTables.length)
    val shelfTable = testDb + "." + shelfTables.head
    assertTrue(s"Shelf table $shelfTable should exist", spark.catalog.tableExists(shelfTable))
  }

  it should "setSemanticHash should set hash when table has no hash" in {
    val tableName = s"$testDb.test_set_hash_new"
    val semanticHash = "newhash123"

    createTestTable(tableName, None)
    assertEquals(None, getSemanticHashFromTable(tableName))

    // Set the semantic hash
    semanticUtils.setSemanticHash(tableName, semanticHash)

    // Verify hash was set
    assertEquals(Some(semanticHash), getSemanticHashFromTable(tableName))
  }

  it should "setSemanticHash should be idempotent when hash already matches" in {
    val tableName = s"$testDb.test_set_hash_same"
    val semanticHash = "samehash123"

    createTestTable(tableName, Some(semanticHash))
    assertEquals(Some(semanticHash), getSemanticHashFromTable(tableName))

    // Setting the same hash should be a no-op
    semanticUtils.setSemanticHash(tableName, semanticHash)

    // Hash should still be the same
    assertEquals(Some(semanticHash), getSemanticHashFromTable(tableName))
  }

  it should "setSemanticHash should throw exception when trying to update existing different hash" in {
    val tableName = s"$testDb.test_set_hash_conflict"
    val oldHash = "oldhash123"
    val newHash = "newhash456"

    createTestTable(tableName, Some(oldHash))
    assertEquals(Some(oldHash), getSemanticHashFromTable(tableName))

    // Attempting to set a different hash should throw IllegalStateException
    val exception = intercept[IllegalStateException] {
      semanticUtils.setSemanticHash(tableName, newHash)
    }

    assertTrue(exception.getMessage.contains("Cannot update the existing semantic hash"))
    assertTrue(exception.getMessage.contains(oldHash))
    assertTrue(exception.getMessage.contains(newHash))
    assertTrue(exception.getMessage.contains("should have been archived"))

    // Original hash should remain unchanged
    assertEquals(Some(oldHash), getSemanticHashFromTable(tableName))
  }

  it should "handle full workflow: check, archive, recreate, and set hash" in {
    val tableName = s"$testDb.test_full_workflow"
    val oldHash = "workflow_hash_v1"
    val newHash = "workflow_hash_v2"

    // Step 1: Create table with old hash
    createTestTable(tableName, Some(oldHash))
    assertTrue(spark.catalog.tableExists(tableName))
    assertEquals(Some(oldHash), getSemanticHashFromTable(tableName))

    // Step 2: Check and archive due to hash change
    val archivedTableOpt = semanticUtils.checkSemanticHashAndArchive(tableName, newHash)
    assertFalse(spark.catalog.tableExists(tableName))

    // Verify archived table is the reuse table
    val expectedReuseTable = tableName + Constants.archiveReuseTableSuffix
    assertEquals(Some(expectedReuseTable), archivedTableOpt)
    assertTrue(s"Archived reuse table $expectedReuseTable should exist", spark.catalog.tableExists(expectedReuseTable))

    // Step 3: Recreate table (simulating batch job creating new table)
    createTestTable(tableName, None)
    assertTrue(spark.catalog.tableExists(tableName))
    assertEquals(None, getSemanticHashFromTable(tableName))

    // Step 4: Set new semantic hash
    semanticUtils.setSemanticHash(tableName, newHash)
    assertEquals(Some(newHash), getSemanticHashFromTable(tableName))

    // Step 5: Subsequent runs with same hash should work
    val noArchiveOpt = semanticUtils.checkSemanticHashAndArchive(tableName, newHash)
    assertEquals(None, noArchiveOpt) // Should not archive
    assertTrue(spark.catalog.tableExists(tableName))
    assertEquals(Some(newHash), getSemanticHashFromTable(tableName))
  }

  it should "handle tables with multiple properties correctly" in {
    val tableName = s"$testDb.test_multiple_props"
    val semanticHash = "multi_prop_hash"

    // Create table with no hash
    createTestTable(tableName, None)

    // Add some other properties first
    tableUtils.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('custom_prop' = 'custom_value', 'another_prop' = 'another_value')")

    // Set semantic hash
    semanticUtils.setSemanticHash(tableName, semanticHash)

    // Verify semantic hash is set
    assertEquals(Some(semanticHash), getSemanticHashFromTable(tableName))

    // Verify other properties are preserved
    val props = tableUtils.getTableProperties(tableName).get
    assertEquals("custom_value", props("custom_prop"))
    assertEquals("another_value", props("another_prop"))
    assertEquals(semanticHash, props(Constants.SemanticHashKey))
  }

  it should "handle special characters in semantic hash" in {
    val tableName = s"$testDb.test_special_chars"
    val specialHash = "hash-with_special.chars:123"

    createTestTable(tableName, None)
    semanticUtils.setSemanticHash(tableName, specialHash)

    assertEquals(Some(specialHash), getSemanticHashFromTable(tableName))

    // Should not archive when hash matches
    val archivedTableOpt = semanticUtils.checkSemanticHashAndArchive(tableName, specialHash)
    assertEquals(None, archivedTableOpt) // Should not archive
    assertTrue(spark.catalog.tableExists(tableName))
  }

  it should "handle empty string semantic hash" in {
    val tableName = s"$testDb.test_empty_hash"
    val emptyHash = ""

    createTestTable(tableName, None)
    semanticUtils.setSemanticHash(tableName, emptyHash)

    assertEquals(Some(emptyHash), getSemanticHashFromTable(tableName))
  }

  it should "handle very long semantic hash strings" in {
    val tableName = s"$testDb.test_long_hash"
    val longHash = "a" * 1000 // 1000 character hash

    createTestTable(tableName, None)
    semanticUtils.setSemanticHash(tableName, longHash)

    assertEquals(Some(longHash), getSemanticHashFromTable(tableName))

    val archivedTableOpt = semanticUtils.checkSemanticHashAndArchive(tableName, longHash)
    assertEquals(None, archivedTableOpt) // Should not archive
    assertTrue(spark.catalog.tableExists(tableName))
  }
}
