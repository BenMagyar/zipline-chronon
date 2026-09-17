/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.other

import ai.chronon.api.Builders
import ai.chronon.spark.submission.SparkSessionBuilder
import ai.chronon.spark.{LogFlattenerJob, LoggingSchema}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

/** Regression test for LogFlattenerJob failing on a corrupt/unknown schema_hash.
  *
  * When the fetcher published a corrupted `LoggableResponse` (a concurrent-encode race garbling the
  * `schemaHash` field into unprintable bytes), the flattener's unguarded `schemaMap(schema_hash)` lookup
  * threw `NoSuchElementException: key not found` and failed the whole batch. The row with the unknown
  * hash should be skipped and counted, not abort the job.
  */
class LogFlattenerSchemaGuardTest extends AnyFlatSpec with BeforeAndAfterAll {

  private implicit lazy val spark: SparkSession =
    SparkSessionBuilder.build("LogFlattenerSchemaGuardTest", local = true)

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private def newJob(): LogFlattenerJob = {
    val joinConf = Builders.Join(
      metaData = Builders.MetaData(name = "unit_test.schema_guard", namespace = "test", team = "chronon")
    )
    new LogFlattenerJob(spark, joinConf, "2022-10-01", "unused_log_table", "unused_schema_table")
  }

  private def rawRow(schemaHash: String): Row =
    Row("", "", 1000L, "2022-10-01", schemaHash)

  // Column names must match what flattenKeyValueBytes selects (key_base64 / value_base64 / ...).
  private val rawSchema = StructType(
    Seq(
      StructField("key_base64", StringType),
      StructField("value_base64", StringType),
      StructField("ts_millis", LongType),
      StructField("ds", StringType),
      StructField("schema_hash", StringType)
    )
  )

  "LogFlattenerJob.flattenKeyValueBytes" should "skip rows whose schema_hash has no matching schema" in {
    // schema_hash contains unprintable control characters, mimicking a corrupted logged record. It is
    // absent from schemaMap, so the previously unguarded lookup threw NoSuchElementException.
    val corruptHash = "abc\u0001\u0002def"
    val rawDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(rawRow(corruptHash))),
      rawSchema
    )

    val flattened = newJob().flattenKeyValueBytes(rawDf, Map.empty[String, LoggingSchema])

    // Job completes and drops the unresolved row instead of throwing.
    flattened.count() shouldBe 0L
  }
}
