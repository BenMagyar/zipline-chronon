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

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{DataFrameGen, SparkTestBase}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import scala.io.Source

class MetadataExporterTest extends SparkTestBase {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  val tableUtils: TableUtils = TableUtils(spark)

  def printFilesInDirectory(directoryPath: String): Unit = {
    val directory = new File(directoryPath)

    if (directory.exists && directory.isDirectory) {
      logger.info("Valid Directory")
      val files = directory.listFiles

      for (file <- files) {
        logger.info(file.getPath)
        if (file.isFile) {
          logger.info(s"File: ${file.getName}")
          val source = Source.fromFile(file)
          val fileContents = source.getLines.mkString("\n")
          source.close()
          logger.info(fileContents)
          logger.info("----------------------------------------")
        }
      }
    } else {
      logger.info("Invalid directory path!")
    }
  }

  it should "metadata export" in {
    // Create the tables.
    val namespace = "example_namespace"
    val tablename = "table"
    SparkTestBase.createDatabase(spark, namespace)
    val sampleData = List(
      Column("a", api.StringType, 10),
      Column("b", api.StringType, 10),
      Column("c", api.LongType, 100),
      Column("d", api.LongType, 100),
      Column("e", api.LongType, 100)
    )
    val sampleTable = s"$namespace.$tablename"
    val sampleDf = DataFrameGen
      .events(spark, sampleData, 10000, partitions = 30)
    sampleDf.save(sampleTable)

    // TODO: Fix this test after cut-over to bazel
//    val confResource = getClass.getResource("/")
//    val tmpDir: File = Files.createTempDir()
//    MetadataExporter.run(confResource.getPath, tmpDir.getAbsolutePath)
//    printFilesInDirectory(s"${confResource.getPath}/joins/team")
//    printFilesInDirectory(s"${tmpDir.getAbsolutePath}/joins")
//    // Read the files.
//    val file = Source.fromFile(s"${tmpDir.getAbsolutePath}/joins/example_join.v1")
//    val jsonString = file.getLines().mkString("\n")
//    val objectMapper = new ObjectMapper()
//    objectMapper.registerModule(DefaultScalaModule)
//    val jsonNode = objectMapper.readTree(jsonString)
//    assertEquals(jsonNode.get("metaData").get("name").asText(), "team.example_join.v1")
  }
}
